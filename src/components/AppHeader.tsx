"use client";
import Image from "next/image";

export default function AppHeader({
  user,
  onLogout,
}: {
  user: { name: string; email: string } | null;
  onLogout: () => void;
}) {
  return (
    <header className="w-full bg-gradient-to-r from-blue-800 via-gray-700 to-orange-600 shadow-md">
      <div className="w-full flex items-center py-4 px-6">
        {/* Logo and Name Leftmost */}
        <div className="flex items-center gap-3 mr-auto">
          <Image
            src="/WinRMALogo.png"
            alt="WinRMA Logo"
            width={80}
            height={80}
            className="object-contain rounded-lg"
          />
          <span className="text-2xl font-extrabold text-white tracking-tight font-[Calibri]">
            WinRMA
          </span>
        </div>

        {/* Title Center */}
        <div className="hidden md:flex flex-1 justify-center">
          <span className="text-lg md:text-xl font-semibold text-gray-100">
            Fast, Seamless, AI-Powered | Legacy to Fabric Power BI Migration
          </span>
        </div>

        {/* User Info Right */}
        <div className="flex items-center gap-4 min-w-[200px] justify-end">
          {user ? (
            <>
              <div className="flex flex-col text-right">
                <span className="text-gray-200 font-medium">
                  Welcome, {user.name || user.email}
                </span>
                <span className="text-sm text-gray-200">User Dashboard</span>
              </div>
               <button
                 onClick={onLogout}
                 className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-all cursor-pointer"
              >
                Logout
              </button>
            </>
          ) : (
            <span className="text-gray-400">Guest</span>
          )}
        </div>
      </div>
    </header>
  );
}
