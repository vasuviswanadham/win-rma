"use client";
import { useState, useRef, useEffect } from "react";

export default function GenerateMetadata() {
  const [selectedMenu, setSelectedMenu] = useState("local");
  const [folderName, setFolderName] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const folderInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (folderInputRef.current) {
      folderInputRef.current.setAttribute("webkitdirectory", "");
    }
  }, [selectedMenu]);

  const handleGenerateMetadata = async () => {
    setIsLoading(true);
    setResult(null);
    setError(null);
    try {
      const response = await fetch('/api/generate-metadata', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ folderName }),
      });
      const data = await response.json();
      if (data.success) {
        setResult(data.output);
      } else {
        setError(data.error || 'Unknown error');
      }
    } catch (err: unknown) {
      if (err instanceof Error) {
        setError(err.message || 'Unknown error');
      } else {
        setError('Unknown error');
      }
    } finally {
      setIsLoading(false);
    }
  };

  const metadataIcon = (
    <span className="inline-block bg-gradient-to-r from-blue-600 to-orange-400 rounded-full p-4 mb-4 shadow-lg">
      <svg width="64" height="64" fill="none" viewBox="0 0 64 64">
        <circle cx="32" cy="32" r="32" fill="#fff" />
        <path d="M20 44V20h24v24H20zm3-3h18V23H23v18zm3-12h12v3H26v-3zm0 6h12v3H26v-3z" fill="#2563eb" />
        <circle cx="32" cy="32" r="6" fill="#f59e42" />
        <rect x="29" y="38" width="6" height="2" rx="1" fill="#2563eb" />
      </svg>
    </span>
  );

  return (
    <div className="flex-1 min-h-0 w-full flex flex-col items-center justify-start bg-gradient-to-br from-blue-100 via-orange-50 to-gray-100 font-[Calibri]">
      <div className="w-[90%] bg-white shadow-2xl rounded-3xl p-2 mt-5 border from-blue-100 via-orange-50 to-gray-100 font-[Calibri] border-gray-200 flex flex-col md:flex-row overflow-x-auto overflow-y-visible">
        {/* Left Menu */}
        <div className="w-72 min-h-[400px] bg-gradient-to-br from-blue-50 via-orange-50 to-gray-50 rounded-2xl shadow flex flex-col items-start p-6 mr-6 flex-shrink-0">
          <h2 className="text-2xl font-bold text-blue-700 mb-6">Source</h2>
          <button
            className={`w-full text-left px-5 py-4 mb-3 rounded-xl font-semibold text-lg transition-all ${selectedMenu === "local" ? "bg-gradient-to-r from-blue-600 to-orange-400 text-white shadow-lg" : "bg-gray-100 text-blue-700 hover:bg-blue-50"}`}
            onClick={() => setSelectedMenu("local")}
          >
            From Local Tableau Folder
          </button>
          <button
            className={`w-full text-left px-5 py-4 rounded-xl font-semibold text-lg transition-all ${selectedMenu === "site" ? "bg-gradient-to-r from-blue-600 to-orange-400 text-white shadow-lg" : "bg-gray-100 text-blue-700 hover:bg-blue-50"}`}
            onClick={() => setSelectedMenu("site")}
          >
            From Tableau Site
          </button>
        </div>

        {/* Right Panel */}
        <div className="flex-1 flex flex-col items-center justify-center min-h-[700px]">
          {metadataIcon}
          <h2 className="text-3xl font-bold text-blue-700 mb-2">Capture Metadata</h2>
          <div className="text-lg text-gray-700 text-center max-w-2xl mb-2">Provides an automated way to extract, transform, and prepare metadata from Tableau dashboards for seamless migration into Power BI</div>

          {selectedMenu === "local" ? (
            <div className="mt-10 w-full max-w-xl flex flex-col items-center">
              <div className="w-full mb-6 flex flex-col items-center">
                <label className="text-xl font-bold text-blue-700 mb-6 text-center w-full">Select Tableau Folder</label>
                <div className="flex flex-row items-center gap-4 w-full justify-center">
                  <label htmlFor="folder-upload" className="border border-gray-300 px-4 py-3 rounded-xl bg-gray-50 text-gray-700 cursor-pointer font-semibold text-center hover:bg-blue-50 focus:outline-none focus:ring-2 focus:ring-blue-500">
                    Choose Folder
                    <input
                      id="folder-upload"
                      type="file"
                      ref={folderInputRef}
                      style={{ display: 'none' }}
                      onChange={e => {
                        const files = e.target.files;
                        if (files && files.length > 0) {
                          const file = files[0] as File;
                          // webkitRelativePath is not standard, so check if it exists
                          let path: string;
                          if ('webkitRelativePath' in file && typeof (file as File & { webkitRelativePath?: unknown }).webkitRelativePath === 'string') {
                            path = (file as File & { webkitRelativePath: string }).webkitRelativePath;
                          } else {
                            path = file.name;
                          }
                          setFolderName(path.split("/")[0]);
                        }
                      }}
                      multiple
                    />
                  </label>
                  <div className="text-sm text-gray-600 text-center">
                    {folderName ? `Selected Folder: ${folderName}` : 'No folder chosen'}
                  </div>
                </div>
              </div>
              <button
                className="bg-gradient-to-r from-blue-600 to-orange-500 text-white px-10 py-4 rounded-2xl font-semibold hover:bg-blue-700 shadow-xl transition-all transform hover:-translate-y-1 hover:scale-105 text-lg tracking-wide cursor-pointer"
                disabled={!folderName || isLoading}
                onClick={handleGenerateMetadata}
              >
                {isLoading ? 'Generating Metadata...' : '🚀 Generate Metadata'}
              </button>
              {isLoading && (
                <div className="mt-4 text-blue-700 font-semibold">Processing, please wait...</div>
              )}
              {result && (
                <div className="mt-6 w-full bg-green-50 border border-green-300 rounded-xl p-6 text-green-800">
                  <div className="text-xl font-bold mb-2">Metadata Generated Successfully and below are the stats:</div>
                  <pre className="whitespace-pre-wrap text-base">{result}</pre>
                </div>
              )}
              {error && (
                <div className="mt-6 w-full bg-red-50 border border-red-300 rounded-xl p-6 text-red-800">
                  <div className="text-xl font-bold mb-2">Error Generating Metadata</div>
                  <pre className="whitespace-pre-wrap text-base">{error}</pre>
                </div>
              )}
            </div>
          ) : (
            <div className="mt-10 flex flex-col items-center justify-center w-full">
              <h2 className="text-2xl font-bold text-orange-700 mb-4">Coming Soon...</h2>
              <div className="text-lg text-gray-600 text-center max-w-xl">This feature will allow you to connect directly to Tableau Site and capture metadata for migration.</div>
            </div>
          )}
        </div>
      </div>
      <style jsx global>{`
        @keyframes slide-right {
          0%, 100% { transform: translateX(0); opacity: 1; }
          50% { transform: translateX(6px); opacity: 0.8; }
        }
        @keyframes slide-down {
          0%, 100% { transform: translateY(0); opacity: 1; }
          50% { transform: translateY(6px); opacity: 0.8; }
        }
        .animate-slide-right {
          animation: slide-right 1.2s infinite ease-in-out;
        }
        .animate-slide-down {
          animation: slide-down 1.2s infinite ease-in-out;
        }
      `}</style>
    </div>
  );
}
