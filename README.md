# Win AIDM Codebase

This is a Next.js app with Azure AD (MSAL) login and a simple landing page.

## Getting Started

1. Copy your Azure AD app registration values into `.env.local`:
   - `NEXT_PUBLIC_AZURE_CLIENT_ID`
   - `NEXT_PUBLIC_AAD_AUTHORITY`
2. Run `npm install` if needed.
3. Start the dev server:
   ```sh
   npm run dev
   ```

## Features
- Microsoft login (MSAL)
- Simple landing page after login

## Project Structure
- `src/components/Login.tsx` — Login button and MSAL logic
- `src/app/page.tsx` — Main landing page

---

For further customization, add your own pages and API routes as needed.
