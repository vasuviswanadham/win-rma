
import { PublicClientApplication, AuthenticationResult } from '@azure/msal-browser';
import { useState } from 'react';
import AppHeader from "./AppHeader";

const msalConfig = {
  auth: {
    clientId: process.env.NEXT_PUBLIC_AZURE_CLIENT_ID || '',
    authority: process.env.NEXT_PUBLIC_AAD_AUTHORITY || 'https://login.microsoftonline.com/common',
    redirectUri: typeof window !== 'undefined' ? window.location.origin : '',
  },
};

const msalInstance = typeof window !== 'undefined' ? new PublicClientApplication(msalConfig) : null;

export default function Login({ onLogin }: { onLogin: (user: { name: string; email: string }, accessToken: string) => void }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleLogin = async () => {
    setError(null);
    setLoading(true);
    try {
      if (!msalInstance) throw new Error('MSAL not initialized');
      await msalInstance.initialize();
      const loginResponse: AuthenticationResult = await msalInstance.loginPopup({
        scopes: ['openid', 'profile', 'email'],
      });
      if (loginResponse && loginResponse.account) {
        onLogin(
          {
            name: loginResponse.account.name || '',
            email: loginResponse.account.username,
          },
          loginResponse.accessToken
        );
      } else {
        setError('Authentication failed.');
      }
    } catch {
      setError('Authentication failed.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen w-full flex flex-col bg-gradient-to-br from-blue-100 via-orange-50 to-gray-100 font-[Calibri]">
      <AppHeader user={null} onLogout={() => {}} />
      <div className="flex flex-1 flex-col items-center justify-center">
        <div className="max-w-xl w-full bg-white shadow-2xl rounded-3xl p-10 flex flex-col items-center mt-10">
          <span className="inline-block bg-gradient-to-r from-blue-600 to-orange-400 rounded-full p-3 mb-6 shadow-lg">
            <svg width="48" height="48" fill="none" viewBox="0 0 48 48"><circle cx="24" cy="24" r="24" fill="#fff"/><path d="M16 32V16h16v16H16zm2-2h12V18H18v12zm2-8h8v2h-8v-2zm0 4h8v2h-8v-2z" fill="#2563eb"/></svg>
          </span>
          <h1 className="text-4xl font-extrabold mb-4 text-center text-gray-800 tracking-tight drop-shadow-lg">Sign in to WinRMA</h1>
          <p className="text-lg text-gray-500 text-center mb-8 max-w-md">Access your migration dashboard and tools by signing in with your Microsoft account.</p>
          <button
            onClick={handleLogin}
            className="bg-gradient-to-r from-blue-600 to-orange-500 text-white px-10 py-4 rounded-2xl font-semibold hover:bg-blue-700 shadow-xl transition-all transform hover:-translate-y-1 hover:scale-105 text-xl tracking-wide mb-4 cursor-pointer"
            disabled={loading}
          >
            {loading ? 'Signing in...' : 'Sign in with Microsoft'}
          </button>
          {error && <div className="text-red-600 mt-2 text-center">{error}</div>}
        </div>
      </div>
    </div>
  );
}
