"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.N8nConverterService = void 0;
const common_1 = require("@nestjs/common");
const config_1 = require("@nestjs/config");
let N8nConverterService = class N8nConverterService {
    constructor(configService) {
        this.configService = configService;
        // Node type mappings from custom types to n8n types
        this.nodeTypeMappings = {
            trigger: 'n8n-nodes-base.webhook',
            sendEmail: 'n8n-nodes-base.gmail',
            wait: 'n8n-nodes-base.wait',
            condition: 'n8n-nodes-base.if',
            getSegmentData: 'n8n-nodes-base.awsS3',
            parseCSV: 'n8n-nodes-base.spreadsheetFile',
            httpRequest: 'n8n-nodes-base.httpRequest',
            code: 'n8n-nodes-base.code',
        };
        // Node type versions
        this.nodeTypeVersions = {
            'n8n-nodes-base.webhook': 2,
            'n8n-nodes-base.gmail': 2,
            'n8n-nodes-base.wait': 1,
            'n8n-nodes-base.if': 2,
            'n8n-nodes-base.awsS3': 1,
            'n8n-nodes-base.spreadsheetFile': 4,
            'n8n-nodes-base.httpRequest': 4,
            'n8n-nodes-base.code': 2,
        };
    }
    /**
     * Build a map of node IDs to unique names
     */
    buildNodeNameMap(nodes) {
        const nameCount = {};
        const nodeNameMap = new Map();
        nodes.forEach((node) => {
            let baseName = node.data.label || this.getDefaultNodeName(node.type);
            nameCount[baseName] = (nameCount[baseName] || 0) + 1;
            const uniqueName = nameCount[baseName] > 1
                ? `${baseName} ${nameCount[baseName]}`
                : baseName;
            nodeNameMap.set(node.id, uniqueName);
        });
        return nodeNameMap;
    }
    /**
     * Convert custom flow format to n8n workflow format
     */
    convertToN8nWorkflow(flowData, campaignName, campaignId) {
        // Build unique name map first
        const nodeNameMap = this.buildNodeNameMap(flowData.nodes);
        const nodes = this.convertNodes(flowData.nodes, campaignId, nodeNameMap);
        const connections = this.convertConnections(flowData.nodes, flowData.edges, nodeNameMap);
        return {
            name: `Campaign: ${campaignName}`,
            nodes,
            connections,
            settings: {
                saveExecutionProgress: true,
                saveManualExecutions: true,
                executionTimeout: 3600, // 1 hour timeout
            },
            staticData: null,
        };
    }
    /**
     * Convert custom nodes to n8n nodes
     */
    convertNodes(nodes, campaignId, nodeNameMap) {
        const gmailCredentialId = this.configService.get('N8N_GMAIL_CREDENTIAL_ID');
        return nodes.map((node) => {
            const n8nType = this.nodeTypeMappings[node.type] || 'n8n-nodes-base.noOp';
            const typeVersion = this.nodeTypeVersions[n8nType] || 1;
            // Get unique name from the pre-built map
            const uniqueName = nodeNameMap.get(node.id) || node.data.label || this.getDefaultNodeName(node.type);
            const n8nNode = {
                id: node.id,
                name: uniqueName,
                type: n8nType,
                typeVersion,
                position: [node.position.x, node.position.y],
                parameters: this.convertNodeParameters(node, campaignId),
            };
            // Add credentials for nodes that require them
            if (node.type === 'sendEmail' && gmailCredentialId) {
                n8nNode.credentials = {
                    gmailOAuth2: {
                        id: gmailCredentialId,
                        name: 'Gmail account',
                    },
                };
            }
            return n8nNode;
        });
    }
    /**
     * Convert node-specific parameters
     */
    convertNodeParameters(node, campaignId) {
        const properties = node.data.properties || {};
        switch (node.type) {
            case 'trigger':
                return {
                    httpMethod: properties.httpMethod || 'POST',
                    path: properties.webhookPath || `campaign-${campaignId}`,
                    responseMode: 'onReceived', // Respond immediately, don't wait for workflow to complete
                    options: {
                        responseData: 'allEntries',
                    },
                };
            case 'sendEmail':
                // Gmail node v2.1 parameters
                // Convert {{variable}} to n8n expression format ={{ $json.variable }}
                const toEmail = this.convertToN8nExpression(properties.to || properties.sendTo || '');
                const emailSubject = this.convertToN8nExpression(properties.subject || '');
                const emailMessage = this.convertToN8nExpression(properties.message || properties.body || '');
                return {
                    resource: 'message',
                    operation: 'send',
                    sendTo: toEmail,
                    subject: emailSubject,
                    emailType: 'text', // or 'html'
                    message: emailMessage,
                    options: {
                        appendAttribution: false,
                    },
                };
            case 'wait':
                return {
                    amount: parseInt(properties.amount) || 1,
                    unit: properties.unit || 'minutes',
                };
            case 'condition':
                // Determine if we're comparing to a boolean
                const isBooleanComparison = properties.value === 'true' || properties.value === 'false';
                const operatorInfo = this.mapConditionOperator(properties.operator, isBooleanComparison);
                return {
                    conditions: {
                        options: {
                            caseSensitive: true,
                            leftValue: '',
                            typeValidation: 'loose', // Changed to loose for better type coercion
                        },
                        conditions: [
                            {
                                id: this.generateId(),
                                leftValue: this.convertToN8nExpression(properties.field || ''),
                                rightValue: isBooleanComparison
                                    ? (properties.value === 'true')
                                    : this.convertToN8nExpression(properties.value || ''),
                                operator: operatorInfo,
                            },
                        ],
                        combinator: 'and',
                    },
                    options: {},
                };
            case 'getSegmentData':
                return {
                    operation: 'download',
                    bucketName: properties.bucket || '',
                    fileKey: properties.key || '',
                    options: {},
                };
            case 'parseCSV':
                return {
                    operation: 'fromFile',
                    options: {
                        delimiter: properties.delimiter || ',',
                        headerRow: properties.hasHeader !== false,
                    },
                };
            case 'httpRequest':
                return {
                    method: properties.method || 'GET',
                    url: this.convertToN8nExpression(properties.url || ''),
                    sendHeaders: !!properties.headers,
                    headerParameters: this.parseJsonSafe(properties.headers),
                    sendBody: !!properties.body,
                    specifyBody: 'json',
                    jsonBody: this.convertToN8nExpression(properties.body || '{}'),
                    options: {},
                };
            case 'code':
                // n8n Code node v2 parameters
                const defaultCode = `// Access input data
const items = $input.all();

// Return processed items
return items;`;
                return {
                    language: 'javaScript',
                    jsCode: properties.jsCode || defaultCode,
                };
            default:
                return {};
        }
    }
    /**
     * Convert edges to n8n connections format
     */
    convertConnections(nodes, edges, nodeNameMap) {
        const connections = {};
        // Group edges by source node
        const edgesBySource = new Map();
        edges.forEach((edge) => {
            const existing = edgesBySource.get(edge.source) || [];
            existing.push(edge);
            edgesBySource.set(edge.source, existing);
        });
        // Convert to n8n format
        edgesBySource.forEach((edgeList, sourceId) => {
            const sourceName = nodeNameMap.get(sourceId);
            if (!sourceName)
                return;
            const mainConnections = [];
            // For condition nodes, we might have multiple outputs (true/false branches)
            const sourceNode = nodes.find(n => n.id === sourceId);
            if ((sourceNode === null || sourceNode === void 0 ? void 0 : sourceNode.type) === 'condition') {
                // Handle condition node with true/false outputs
                const trueEdges = [];
                const falseEdges = [];
                edgeList.forEach((edge) => {
                    const targetName = nodeNameMap.get(edge.target);
                    if (!targetName)
                        return;
                    const connection = { node: targetName, type: 'main', index: 0 };
                    // Check sourceHandle for true/false branch
                    if (edge.sourceHandle === 'false') {
                        falseEdges.push(connection);
                    }
                    else {
                        trueEdges.push(connection);
                    }
                });
                mainConnections.push(trueEdges);
                mainConnections.push(falseEdges);
            }
            else {
                // Standard node with single output
                const outputConnections = [];
                edgeList.forEach((edge) => {
                    const targetName = nodeNameMap.get(edge.target);
                    if (!targetName)
                        return;
                    outputConnections.push({ node: targetName, type: 'main', index: 0 });
                });
                mainConnections.push(outputConnections);
            }
            connections[sourceName] = { main: mainConnections };
        });
        return connections;
    }
    /**
     * Convert {{variable}} format to ={{$json.variable}} n8n format
     */
    convertVariables(text) {
        if (!text)
            return text;
        // Convert {{variableName}} to ={{ $json.variableName }}
        return text.replace(/\{\{(\w+)\}\}/g, '={{ $json.$1 }}');
    }
    /**
     * Convert template variables to n8n expression format
     * Handles {{variable}} -> ={{ $json.body.variable }}
     * n8n webhook places POST body data under $json.body
     */
    convertToN8nExpression(text) {
        if (!text || typeof text !== 'string')
            return text || '';
        // Check if the text contains any template variables
        const hasVariables = /\{\{[\w.]+\}\}/.test(text);
        if (!hasVariables) {
            return text;
        }
        // Replace all {{variableName}} with {{ $json.body.variableName }}
        // n8n webhook receives POST data under $json.body
        let result = text.replace(/\{\{([\w.]+)\}\}/g, '{{ $json.body.$1 }}');
        // Wrap entire string as expression if it contains variables
        // n8n expressions start with =
        if (result !== text) {
            result = '=' + result;
        }
        return result;
    }
    /**
     * Map custom condition operators to n8n operators
     */
    mapConditionOperator(operator, isBoolean = false) {
        if (isBoolean) {
            const boolOperatorMap = {
                equals: { type: 'boolean', operation: 'equals' },
                notEquals: { type: 'boolean', operation: 'notEquals' },
            };
            return boolOperatorMap[operator] || { type: 'boolean', operation: 'equals' };
        }
        const operatorMap = {
            equals: { type: 'string', operation: 'equals' },
            notEquals: { type: 'string', operation: 'notEquals' },
            contains: { type: 'string', operation: 'contains' },
            greaterThan: { type: 'number', operation: 'gt' },
            lessThan: { type: 'number', operation: 'lt' },
        };
        return operatorMap[operator] || { type: 'string', operation: 'equals' };
    }
    /**
     * Get default node name for a type
     */
    getDefaultNodeName(type) {
        const nameMap = {
            trigger: 'Webhook Trigger',
            sendEmail: 'Send Email',
            wait: 'Wait',
            condition: 'IF Condition',
            getSegmentData: 'Get Segment Data',
            parseCSV: 'Parse CSV',
            httpRequest: 'HTTP Request',
            code: 'Code',
        };
        return nameMap[type] || type;
    }
    /**
     * Safely parse JSON string
     */
    parseJsonSafe(jsonString) {
        if (!jsonString)
            return {};
        try {
            const parsed = JSON.parse(jsonString);
            // Convert to n8n header format
            if (typeof parsed === 'object') {
                return {
                    parameters: Object.entries(parsed).map(([name, value]) => ({
                        name,
                        value,
                    })),
                };
            }
            return {};
        }
        catch (_a) {
            return {};
        }
    }
    /**
     * Generate a random ID
     */
    generateId() {
        return Math.random().toString(36).substring(2, 15);
    }
    /**
     * Validate flow data before conversion
     */
    validateFlowData(flowData) {
        const errors = [];
        if (!flowData.nodes || flowData.nodes.length === 0) {
            errors.push('Flow must have at least one node');
        }
        // Check for trigger node
        const hasTrigger = flowData.nodes.some((n) => n.type === 'trigger');
        if (!hasTrigger) {
            errors.push('Flow must have a trigger node');
        }
        // Check for orphan nodes (nodes without connections)
        const connectedNodes = new Set();
        flowData.edges.forEach((edge) => {
            connectedNodes.add(edge.source);
            connectedNodes.add(edge.target);
        });
        const triggerNodes = flowData.nodes.filter((n) => n.type === 'trigger');
        flowData.nodes.forEach((node) => {
            // Trigger nodes can be unconnected on input side
            if (node.type === 'trigger')
                return;
            if (!connectedNodes.has(node.id)) {
                errors.push(`Node "${node.data.label || node.type}" is not connected`);
            }
        });
        // Check for cycles (basic check)
        // More sophisticated cycle detection could be added
        return {
            valid: errors.length === 0,
            errors,
        };
    }
};
exports.N8nConverterService = N8nConverterService;
exports.N8nConverterService = N8nConverterService = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [config_1.ConfigService])
], N8nConverterService);
