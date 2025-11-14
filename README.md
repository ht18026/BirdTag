<div align="center">

# 🐦 BirdTag

### Intelligent Bird Media Management Platform

*An AWS-powered serverless media storage system with AI-driven bird detection and advanced tagging capabilities*

[Setup Guide](https://www.notion.so/Project-Setup-20b7b5d70392801b92e4fc67dad6df4d?source=copy_link) • [User Guide](userGuide.pdf) • [Report Issues](#)

---

</div>

## 📖 Overview

BirdTag is a cutting-edge, serverless media management platform designed for bird enthusiasts, researchers, and conservationists. Built on AWS infrastructure, it automatically detects and tags birds in images, videos, and audio files using state-of-the-art machine learning models. Whether you're documenting local bird populations or managing large ornithological datasets, BirdTag makes it effortless to organize, search, and discover your bird media.

### ✨ Key Features

- **🤖 Intelligent Bird Detection** - Automatic identification of bird species in images, videos, and audio using advanced ML models
- **📸 Multi-Format Support** - Seamlessly handles images (JPG, PNG), videos (MP4, AVI), and audio files (MP3, WAV)
- **🔍 Smart Search & Filtering** - Query your media library by bird species, confidence levels, file types, and custom tags
- **🔔 Real-time Notifications** - Subscribe to specific bird species and receive email alerts when new matches are uploaded
- **🔐 Secure Authentication** - Enterprise-grade security with AWS Cognito user management
- **⚡ Serverless Architecture** - Scalable, cost-efficient, and maintenance-free infrastructure
- **🖼️ Automatic Thumbnails** - Optimized preview generation for quick browsing
- **📊 Rich Metadata** - Comprehensive detection data including confidence scores and timestamps

---

## 🏗️ Architecture

BirdTag leverages a modern serverless architecture built entirely on AWS services:

### Core Components

#### **Authentication Layer**
- **AWS Cognito** manages secure user signup, login, and JWT token issuance
- Frontend seamlessly integrates authentication flows
- Lambda functions validate tokens before processing secure requests

#### **Upload & Processing Pipeline**
1. **Pre-signed URL Generation** - Lambda creates secure, time-limited S3 upload URLs
2. **Event-Driven Processing** - S3 upload events trigger automated detection workflows
3. **AI Detection** - Specialized Lambda functions analyze media using:
   - **BirdNET-Analyzer** for audio bird call recognition
   - **Custom ML models** for image and video bird detection
4. **Thumbnail Creation** - Optimized previews generated and stored in S3
5. **Metadata Storage** - Detection results and file metadata persisted in DynamoDB

#### **Network Architecture**
- **VPC Isolation** - Secure private and public subnet configuration
- **NAT Gateway** - Controlled outbound internet access for private resources
- **VPC Endpoints** - S3 and DynamoDB traffic stays within AWS backbone for enhanced security and performance

#### **Query & Retrieval System**
- **DynamoDB** serves as the primary database with optimized secondary indexes
- **Query Lambda Functions** handle:
  - Tag-based searches with confidence thresholds
  - File type filtering (image/video/audio)
  - CRUD operations on media records
  - Thumbnail URL retrieval

#### **API Gateway**
- Centralized REST API endpoint
- Request validation and throttling
- Routes requests to appropriate Lambda functions
- CORS configuration for web frontend

#### **Notification System**
- **SNS Topics** manage tag-based subscription filters
- **Subscription Lambda** publishes notifications when specific birds are detected
- Users receive email alerts for species of interest

---

## 🚀 Getting Started

### Prerequisites

- AWS Account with appropriate permissions
- AWS CLI configured
- Python 3.9+ for Lambda functions
- Node.js (optional, for local frontend development)

### Quick Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/BirdTag.git
   cd BirdTag
   ```

2. **Deploy AWS Infrastructure**

   Follow the comprehensive [setup tutorial](https://www.notion.so/Project-Setup-20b7b5d70392801b92e4fc67dad6df4d?source=copy_link) for detailed CloudFormation deployment instructions.

3. **Configure Frontend**

   Update the configuration in `index.html`:
   ```javascript
   const CONFIG = {
       API_ENDPOINT: 'your-api-gateway-url',
       COGNITO_DOMAIN: 'your-cognito-domain',
       COGNITO_CLIENT_ID: 'your-client-id',
       S3_BUCKET: 'your-bucket-name'
   };
   ```

4. **Access the Application**

   Open `index.html` in your browser or deploy to S3 with CloudFront for production use.

---

## 📚 Usage

### Uploading Media

1. **Sign in** with your AWS Cognito credentials
2. **Click the upload area** or drag-and-drop your bird media files
3. **Wait for processing** - Detection typically completes within seconds
4. **View results** - Browse detected species with confidence scores

### Searching Your Library

- **By Species**: Enter bird names (e.g., "crow", "robin", "eagle")
- **By Confidence**: Set minimum detection confidence thresholds
- **By File Type**: Filter by images, videos, or audio
- **View All**: Browse your complete media library

### Setting Up Notifications

1. Navigate to the subscription settings
2. Select bird species you want to track
3. Receive email alerts when matching birds are detected in new uploads

---

## 🛠️ Technology Stack

### Cloud Services
- **AWS Lambda** - Serverless compute for all backend logic
- **Amazon S3** - Scalable object storage for media files
- **Amazon DynamoDB** - NoSQL database for metadata
- **AWS Cognito** - User authentication and authorization
- **Amazon API Gateway** - REST API management
- **Amazon SNS** - Notification and pub/sub messaging
- **AWS VPC** - Network isolation and security

### Machine Learning
- **BirdNET-Analyzer** - Audio bird species identification
- **Custom Detection Models** - Image and video bird recognition

### Frontend
- **Vanilla JavaScript** - Lightweight, dependency-free UI
- **HTML5 & CSS3** - Modern, responsive interface

---

## 📁 Project Structure

```
BirdTag/
├── bird_detection/          # ML models for image/video detection
├── BirdNET-Analyzer/        # Audio bird detection system
├── lambda_audio_detection/  # Audio processing Lambda
├── lambda_image_detection/  # Image processing Lambda
├── lambda_video_detection/  # Video processing Lambda
├── lambda_sns/              # Notification Lambda
├── query_functions/         # Search and retrieval Lambdas
├── upload_file/             # Pre-signed URL generation
├── configuration/           # CloudFormation templates
├── index.html               # Web application frontend
└── userGuide.pdf           # Comprehensive user documentation
```

---

## 🔒 Security

- **Encryption**: All data encrypted at rest (S3, DynamoDB) and in transit (HTTPS/TLS)
- **Authentication**: JWT token-based authentication via AWS Cognito
- **Authorization**: Fine-grained IAM policies for least-privilege access
- **Network Isolation**: VPC with private subnets for Lambda functions
- **Input Validation**: API Gateway validates all requests

---

## 🤝 Contributing

Contributions are welcome! Whether it's bug fixes, feature enhancements, or documentation improvements:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is open source and available under the MIT License.

---

## 🙏 Acknowledgments

- **BirdNET** - For the exceptional audio bird detection model
- **AWS** - For the robust serverless infrastructure
- **The ornithology community** - For inspiring this project

---

<div align="center">

**Made with ❤️ for bird enthusiasts everywhere**

[⬆ Back to Top](#-birdtag)

</div>
