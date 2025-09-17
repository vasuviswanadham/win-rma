"use client";
import { useState } from "react";
// import Image from "next/image";
import Login from "../components/Login";
import ProjectOverview from "../components/ProjectOverview";
import ReportMigrator from "../components/ReportMigrator";
import ReportSummary from "../components/ReportSummary";
import AppHeader from "../components/AppHeader";
import GenerateMetadata from "../components/GenerateMetadata";

export default function Home() {
  const [user, setUser] = useState<{ name: string; email: string } | null>(null);
  const [step, setStep] = useState<'login' | 'overview' | 'migrator' | 'summary' | 'metadata'>('login');

  const handleLogout = () => {
    setUser(null);
    setStep('login');
  };

  if (!user) {
    return <Login onLogin={(u) => { setUser(u); setStep('overview'); }} />;
  }

  return (
    <div className="h-screen min-h-0 flex flex-col overflow-hidden">
      {/* App Header */}
      <AppHeader user={user} onLogout={handleLogout} />
      {/* Top Navigation */}
      <nav className="w-full bg-gray-100 border-b flex flex-row py-4 px-8 items-center flex-shrink-0">
        <button
          className={`mr-4 px-4 py-2 rounded-lg font-semibold transition-all cursor-pointer ${step === 'overview' ? 'bg-[#00446c] text-white' : 'bg-[#297DB0] text-white hover:bg-[#065888]'}`}
          onClick={() => setStep('overview')}
        >
          Home
        </button>
        <button
          className={`mr-4 px-4 py-2 rounded-lg font-semibold transition-all cursor-pointer ${step === 'metadata' ? 'bg-[#00446c] text-white' : 'bg-[#297DB0] text-white hover:bg-[#065888]'}`}
          onClick={() => setStep('metadata')}
        >
          Generate Metadata
        </button>
        <button
          className={`mr-4 px-4 py-2 rounded-lg font-semibold transition-all cursor-pointer ${step === 'summary' ? 'bg-[#00446c] text-white' : 'bg-[#297DB0] text-white hover:bg-[#065888]'}`}
          onClick={() => setStep('summary')}
        >
          Report Summary
        </button>
        <button
          className={`px-4 py-2 rounded-lg font-semibold transition-all cursor-pointer ${step === 'migrator' ? 'bg-[#00446c] text-white' : 'bg-[#297DB0] text-white hover:bg-[#065888]'}`}
          onClick={() => setStep('migrator')}
        >
          Report Migrator
        </button>
      </nav>
      {/* Main Content */}
      <div className="flex-1 min-h-0 overflow-hidden flex flex-col">
        {step === 'overview' ? (
          <div className="flex-1 min-h-0 flex flex-col items-center justify-center overflow-auto">
            <ProjectOverview onNext={() => {
              setStep('migrator');
            }} />
          </div>
        ) : step === 'metadata' ? (
          <div className="flex-1 min-h-0 flex flex-col items-center justify-center overflow-auto">
            {/* Generate Metadata Page */}
            <GenerateMetadata />
          </div>
        ) : step === 'migrator' ? (
          <div className="flex-1 min-h-0 overflow-auto">
            <ReportMigrator onBack={() => setStep('summary')} />
          </div>
        ) : step === 'summary' ? (
          <div className="flex-1 min-h-0 overflow-auto">
            <ReportSummary />
          </div>
        ) : (
          <div className="flex-1 min-h-0 flex flex-col items-center justify-center overflow-auto">
            <h1 className="text-3xl font-bold mb-6">Welcome, {user.name || user.email}!</h1>
            <p className="mb-4">You are signed in with your Microsoft account.</p>
          </div>
        )}
      </div>
    </div>
  );
}
