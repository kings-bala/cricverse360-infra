import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as rds from "aws-cdk-lib/aws-rds";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as iam from "aws-cdk-lib/aws-iam";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import { Construct } from "constructs";
import * as path from "path";

export class CricVerse360Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ─── Dedicated VPC (isolated from existing resources) ───
    const vpc = new ec2.Vpc(this, "CricVerse360Vpc", {
      vpcName: "cricverse360-vpc",
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: "public",
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: "isolated",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
        },
      ],
    });

    // ─── Security Groups ───
    const dbSecurityGroup = new ec2.SecurityGroup(this, "DbSecurityGroup", {
      vpc,
      securityGroupName: "cricverse360-db-sg",
      description: "Security group for Aurora Serverless v2",
      allowAllOutbound: false,
    });

    const lambdaSecurityGroup = new ec2.SecurityGroup(
      this,
      "LambdaSecurityGroup",
      {
        vpc,
        securityGroupName: "cricverse360-lambda-sg",
        description: "Security group for Lambda functions",
        allowAllOutbound: true,
      }
    );

    dbSecurityGroup.addIngressRule(
      lambdaSecurityGroup,
      ec2.Port.tcp(5432),
      "Allow Lambda to connect to Aurora"
    );

    // ─── Aurora Serverless v2 (PostgreSQL, scales to 0) ───
    const dbCluster = new rds.DatabaseCluster(this, "CricVerse360Db", {
      engine: rds.DatabaseClusterEngine.auroraPostgres({
        version: rds.AuroraPostgresEngineVersion.VER_16_6,
      }),
      serverlessV2MinCapacity: 0,
      serverlessV2MaxCapacity: 1,
      writer: rds.ClusterInstance.serverlessV2("writer", {
        publiclyAccessible: false,
      }),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      securityGroups: [dbSecurityGroup],
      defaultDatabaseName: "cricverse360",
      enableDataApi: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      credentials: rds.Credentials.fromGeneratedSecret("cricverse360admin", {
        secretName: "cricverse360/db-credentials",
      }),
    });

    // ─── Cognito User Pool ───
    const userPool = new cognito.UserPool(this, "CricVerse360UserPool", {
      userPoolName: "cricverse360-users",
      selfSignUpEnabled: true,
      signInAliases: { email: true },
      autoVerify: { email: true },
      standardAttributes: {
        email: { required: true, mutable: true },
        fullname: { required: true, mutable: true },
      },
      customAttributes: {
        role: new cognito.StringAttribute({ mutable: true }),
        academy: new cognito.StringAttribute({ mutable: true }),
      },
      passwordPolicy: {
        minLength: 8,
        requireLowercase: true,
        requireUppercase: true,
        requireDigits: true,
        requireSymbols: false,
      },
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const userPoolClient = new cognito.UserPoolClient(
      this,
      "CricVerse360AppClient",
      {
        userPool,
        userPoolClientName: "cricverse360-web",
        authFlows: {
          userPassword: true,
          userSrp: true,
        },
        generateSecret: false,
        accessTokenValidity: cdk.Duration.hours(1),
        idTokenValidity: cdk.Duration.hours(1),
        refreshTokenValidity: cdk.Duration.days(30),
      }
    );

    // ─── S3 Bucket (profile pics, uploads) ───
    const bucket = new s3.Bucket(this, "CricVerse360Bucket", {
      bucketName: `cricverse360-assets-${this.account}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      cors: [
        {
          allowedMethods: [
            s3.HttpMethods.GET,
            s3.HttpMethods.PUT,
            s3.HttpMethods.POST,
          ],
          allowedOrigins: [
            "https://cricverse360.com",
            "https://*.vercel.app",
            "http://localhost:3000",
          ],
          allowedHeaders: ["*"],
          maxAge: 3600,
        },
      ],
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    // ─── Lambda Function (API handler) ───
    const apiHandler = new lambda.Function(this, "CricVerse360ApiHandler", {
      functionName: "cricverse360-api",
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda")),
      memorySize: 256,
      timeout: cdk.Duration.seconds(60),
      environment:{
        DB_CLUSTER_ARN: dbCluster.clusterArn,
        DB_SECRET_ARN: dbCluster.secret?.secretArn || "",
        DB_NAME: "cricverse360",
        USER_POOL_ID: userPool.userPoolId,
        USER_POOL_CLIENT_ID: userPoolClient.userPoolClientId,
        BUCKET_NAME: bucket.bucketName,
        REGION: this.region,
        GEMINI_API_KEY: process.env.GEMINI_API_KEY || "",
        STRIPE_SECRET_KEY: process.env.STRIPE_SECRET_KEY || "",
        STRIPE_PRO_PRICE_ID: process.env.STRIPE_PRO_PRICE_ID || "",
        STRIPE_PRO_PLUS_PRICE_ID: process.env.STRIPE_PRO_PLUS_PRICE_ID || "",
        STRIPE_ONE_TIME_PRICE_ID: process.env.STRIPE_ONE_TIME_PRICE_ID || "",
        STRIPE_WEBHOOK_SECRET: process.env.STRIPE_WEBHOOK_SECRET || "",
      },
    });

    dbCluster.grantDataApiAccess(apiHandler);
    bucket.grantReadWrite(apiHandler);

    apiHandler.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "cognito-idp:AdminCreateUser",
          "cognito-idp:AdminSetUserPassword",
          "cognito-idp:AdminUpdateUserAttributes",
          "cognito-idp:AdminGetUser",
          "cognito-idp:AdminDisableUser",
          "cognito-idp:AdminEnableUser",
          "cognito-idp:ListUsers",
          "cognito-idp:InitiateAuth",
          "cognito-idp:AdminInitiateAuth",
          "cognito-idp:SignUp",
          "cognito-idp:ConfirmSignUp",
          "cognito-idp:ForgotPassword",
          "cognito-idp:ConfirmForgotPassword",
          "cognito-idp:GetUser",
        ],
        resources: [userPool.userPoolArn],
      })
    );

    // ─── API Gateway ───
    const api = new apigateway.RestApi(this, "CricVerse360Api", {
      restApiName: "cricverse360-api",
      description: "CricVerse360 Backend API",
      defaultCorsPreflightOptions: {
        allowOrigins: [
          "https://cricverse360.com",
          "https://*.vercel.app",
          "http://localhost:3000",
        ],
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: [
          "Content-Type",
          "Authorization",
          "X-Amz-Date",
          "X-Api-Key",
          "X-User-Email",
          "X-User-Name",
        ],
        allowCredentials: true,
      },
      deployOptions: {
        stageName: "v1",
        throttlingRateLimit: 100,
        throttlingBurstLimit: 200,
      },
    });

    const lambdaIntegration = new apigateway.LambdaIntegration(apiHandler);

    // Proxy integration: single {proxy+} catches all routes (avoids Lambda policy size limit)
    api.root.addMethod("ANY", lambdaIntegration);
    api.root.addProxy({
      defaultIntegration: lambdaIntegration,
      anyMethod: true,
    });

    // ─── Outputs ───
    new cdk.CfnOutput(this, "ApiUrl", {
      value: api.url,
      description: "API Gateway URL",
    });

    new cdk.CfnOutput(this, "UserPoolId", {
      value: userPool.userPoolId,
      description: "Cognito User Pool ID",
    });

    new cdk.CfnOutput(this, "UserPoolClientId", {
      value: userPoolClient.userPoolClientId,
      description: "Cognito User Pool Client ID",
    });

    new cdk.CfnOutput(this, "BucketName", {
      value: bucket.bucketName,
      description: "S3 Bucket Name",
    });

    new cdk.CfnOutput(this, "DbClusterArn", {
      value: dbCluster.clusterArn,
      description: "Aurora Cluster ARN",
    });

    new cdk.CfnOutput(this, "VpcId", {
      value: vpc.vpcId,
      description: "VPC ID",
    });
  }
}
