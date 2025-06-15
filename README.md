# BirdTag
## Description
An AWS-powered Serverless Media Storage System with Advanced Tagging Capabilities

## Setup tutorial
*https://www.notion.so/Project-Setup-20b7b5d70392801b92e4fc67dad6df4d?source=copy_link*

## Technical Implementation
### Authentication
AWS Cognito handles user signup, confirmation, and token issuance. The frontend integrates Cognito for login, while backend Lambda functions validate tokens before processing secured requests.
### File Upload Pipeline
Upload Lambda generates pre-signed URLs for secure S3 uploads. Files are stored into different prefixes based on the suffix of the file. Lambda automatically activates upon file upload event to initiate the detection workflow that creates optimized thumbnails stored in S3 and downloads corresponding models to automatically extract bird-related tags and store all information as a record in DynamoDB.
VPC Network Architecture
The system operates within a secure VPC featuring private and public subnets for network isolation. NAT Gateway provides secure outbound internet access for private resources. VPC Endpoints for S3 and DynamoDB ensure traffic remains within AWS backbone for enhanced security and performance.
### Query System
Query Lambda handles complex search operations, interfacing with DynamoDB for fetch, add, and delete operations. Query by File Type Lambda enables filtering based on media types. DynamoDB serves as the primary database storing file metadata, tags, and user queries with optimized indexing.
API Gateway and Routing
API Gateway serves as the central entry point, routing upload requests and queries to appropriate Lambda functions with request validation and throttling capabilities.
### Notification System
SNS Topics manage tag-based subscriptions based on filters while another SNS Subscription Lambda publishes notifications when specific tags are detected in new uploads. Users can subscribe to bird tags of interest in the UI and receive email notifications when matching content is uploaded.

## architecture diagram
## User guidance
please see userGuide.pdf
