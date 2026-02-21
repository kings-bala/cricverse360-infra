#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { CricVerse360Stack } from "../lib/cricverse360-stack";

const app = new cdk.App();
new CricVerse360Stack(app, "CricVerse360Stack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || "us-east-1",
  },
});
