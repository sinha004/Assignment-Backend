"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const core_1 = require("@nestjs/core");
const common_1 = require("@nestjs/common");
const swagger_1 = require("@nestjs/swagger");
const app_module_1 = require("./app.module");
const http_exception_filter_1 = require("./common/filters/http-exception.filter");
async function bootstrap() {
    const app = await core_1.NestFactory.create(app_module_1.AppModule);
    // Enable CORS for frontend
    app.enableCors({
        origin: ['http://localhost:3000', 'https://ckgik5oh3l.execute-api.us-east-1.amazonaws.com', 'https://campaignify-frontend.vercel.app'],
        credentials: true,
    });
    // Global validation pipe
    app.useGlobalPipes(new common_1.ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
    }));
    // Global exception filter
    app.useGlobalFilters(new http_exception_filter_1.HttpExceptionFilter());
    // Swagger configuration
    const config = new swagger_1.DocumentBuilder()
        .setTitle('Marketing Campaign API')
        .setDescription('API documentation for Marketing Campaign Management System')
        .setVersion('1.0')
        .addBearerAuth({
        type: 'http',
        scheme: 'bearer',
        bearerFormat: 'JWT',
        name: 'JWT',
        description: 'Enter JWT token',
        in: 'header',
    }, 'JWT-auth')
        .build();
    const document = swagger_1.SwaggerModule.createDocument(app, config);
    swagger_1.SwaggerModule.setup('api', app, document, {
        customSiteTitle: 'Marketing API Docs',
        customCss: '.swagger-ui .topbar { display: none }',
    });
    await app.listen(5000);
    console.log('🚀 Backend server running on http://localhost:5000');
    console.log('📚 Swagger API Docs available at http://localhost:5000/api');
}
bootstrap();
