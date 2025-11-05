# TOS Privacy Policy Crawler - Backend API

FastAPI backend for the TOS Privacy Policy Crawler application.

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- PostgreSQL database
- Redis (optional for local dev)
- Firebase Admin SDK credentials

### Installation

1. **Navigate to backend directory**
   ```bash
   cd backend
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Run database migrations**
   ```bash
   alembic upgrade head
   ```

6. **Start development server**
   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

7. **Access API documentation**
   - Swagger UI: http://localhost:8000/docs
   - ReDoc: http://localhost:8000/redoc

## 📁 Project Structure

```
backend/
├── app/
│   ├── api/           # API routes
│   ├── models/        # Database models
│   ├── schemas/       # Pydantic schemas
│   ├── services/      # Business logic
│   ├── database/      # Database configuration
│   ├── middleware/    # Custom middleware
│   └── main.py        # FastAPI app
├── alembic/          # Database migrations
├── requirements.txt
└── .env              # Environment variables
```

## 🔧 Environment Variables

See `.env.example` for all required environment variables.

Key variables:
- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string
- `FIREBASE_PROJECT_ID`: Firebase project ID
- `FIREBASE_PRIVATE_KEY`: Firebase Admin SDK private key
- `FIREBASE_CLIENT_EMAIL`: Firebase service account email
- `GEMINI_API_KEY`: Google Gemini API key

## 📚 API Documentation

Once running, visit:
- http://localhost:8000/docs (Swagger UI)
- http://localhost:8000/redoc (ReDoc)

## 🧪 Testing

Phase-by-phase testing guide:
- Phase 1: Foundation (Auth, Database, Basic API)
- Phase 2: Crawling (Web scraping, document extraction)
- Phase 3: Analysis (Gemini integration, text mining)
- Phase 4: Advanced features (Caching, rate limiting)
- Phase 5: Production deployment

## 🚢 Deployment (Render)

See `PHASE1_TESTING_GUIDE.md` for Render deployment instructions.

