"use client";


export default function ProjectOverview({ onNext }: { onNext: (company: string, project: string) => void }) {
  return (
    <div className="flex-1 min-h-full flex flex-col items-center justify-start w-full bg-gradient-to-br from-blue-100 via-orange-50 to-gray-100 font-[Calibri]">
      <div className="w-[90%] bg-white shadow-2xl rounded-3xl p-2 mt-5 border from-blue-100 via-orange-50 to-gray-100 font-[Calibri] border-gray-200 overflow-x-auto overflow-y-visible">
        <div className="flex flex-col items-center mb-2 ">
          <span className="inline-block bg-gradient-to-r from-blue-600 to-orange-400 rounded-full p-3 mb-4 shadow-lg">
            <svg width="48" height="48" fill="none" viewBox="0 0 48 48"><circle cx="24" cy="24" r="24" fill="#fff" /><path d="M16 32V16h16v16H16zm2-2h12V18H18v12zm2-8h8v2h-8v-2zm0 4h8v2h-8v-2z" fill="#2563eb" /></svg>
          </span>
          <h1 className="text-5xl font-extrabold text-center text-gray-800 mb-1 tracking-tight drop-shadow-lg"><span className="bg-gradient-to-r from-blue-400 via-orange-400 to-gray-500 bg-clip-text text-transparent font-bold">
            Report Migration Accelerator
          </span></h1>
          <p className="text-lg text-gray-500 text-center max-w-2xl">
            AI-powered migration from Legacy reporting platform to Fabric Power BI, fast, intelligent, and worry-free.
          </p>
        </div>

        <div className="bg-gradient-to-r from-blue-50 via-orange-50 to-gray-50 rounded-xl mb-4 shadow flex flex-col items-center w-full max-w-full">

          <p className="text-lg text-gray-700 text-left max-w-7xl mb-2 pt-2">
            <strong>Lightning-Fast Migration to Microsoft Fabric Power BI</strong>
            <br />
            Accelerate your journey from legacy reporting platforms to{' '}
            <strong>Fabric Power BI</strong> with next-generation automation. Our
            solution, powered by the latest <strong>AI GPT models</strong>,
            intelligently parses metadata, data models, calculations, and visuals&mdash;delivering
            up to <strong>75% automation</strong> in the migration process. <br />
            By standardizing and streamlining migration with AI, enterprises can achieve:
          </p>
          <ul className="text-gray-700 text-left max-w-7xl list-disc list-inside mb-1 pl-25">
            <li>
              <strong>75% reduction in migration effort</strong> &rarr; lower costs and faster delivery
            </li>
            <li>
              <strong>Minimal downtime</strong> &rarr; ensuring business continuity
            </li>
            <li>
              <strong>Faster ROI</strong> &rarr; quickly unlock the full value of Power BI
            </li>
          </ul>
          <p className="text-lg text-gray-700 text-left max-w-7xl mb-2">
            Confidently modernize your analytics with a solution designed to maximize efficiency,
            reduce complexity, and enable <strong>faster, data-driven insights</strong> with Fabric Power BI.
          </p>
         
        </div>

        {/* Add this in your global CSS */}
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

        <div className="flex flex-col md:flex-row items-center justify-center gap-6 mb-5 w-full max-w-full overflow-x-auto">
          {/* Box 1 */}
          <div className="bg-gradient-to-br from-blue-50 to-blue-100 rounded-2xl shadow p-6 flex flex-col items-center h-40 min-w-[260px] w-full md:w-80 max-w-xs flex-shrink-0">
            <span className="bg-blue-600 text-white rounded-full mb-3">
              <svg width="32" height="32" fill="none" viewBox="0 0 32 32">
                <circle cx="16" cy="16" r="16" fill="#2563eb" />
                <rect x="9" y="11" width="10" height="12" rx="2" fill="#fff" />
                <circle cx="21" cy="23" r="4" fill="#fff" stroke="#2563eb" strokeWidth="2" />
                <rect x="20.5" y="25.5" width="3" height="1.5" rx="0.75" transform="rotate(-45 20.5 25.5)" fill="#2563eb" />
              </svg>
            </span>
            <h2 className="text-xl font-bold text-blue-700 mb-1">Metadata Analyzer</h2>
            <p className="text-gray-600 text-center">Extracts Tableau metadata and prepares it for migration.</p>
          </div>

          {/* Arrow */}
          <div className="w-fit h-fit flex justify-center items-center">
            {/* Desktop → Sliding Right Arrow */}
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-7 w-7 text-blue-500 hidden md:block animate-slide-right"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2.5}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M4 12h16m-6-6l6 6-6 6" />
            </svg>

            {/* Mobile → Sliding Down Arrow */}
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-7 w-7 text-blue-500 block md:hidden animate-slide-down"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2.5}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m6-6l-6 6-6-6" />
            </svg>
          </div>

          {/* Box 2 */}
          <div className="bg-gradient-to-br from-orange-50 to-orange-100 rounded-2xl shadow p-6 flex flex-col items-center h-40 min-w-[260px] w-full md:w-80 max-w-xs flex-shrink-0">
            <span className="bg-orange-500 text-white rounded-full mb-3">
              <svg width="32" height="32" fill="none" viewBox="0 0 32 32">
                <circle cx="16" cy="16" r="16" fill="#f59e42" />
                <rect x="10" y="10" width="12" height="12" rx="3" fill="#fff" />
                <circle cx="16" cy="16" r="3" fill="#f59e42" />
                <circle cx="13" cy="13" r="1" fill="#f59e42" />
                <circle cx="19" cy="13" r="1" fill="#f59e42" />
                <circle cx="13" cy="19" r="1" fill="#f59e42" />
                <circle cx="19" cy="19" r="1" fill="#f59e42" />
              </svg>
            </span>
            <h2 className="text-xl font-bold text-orange-700 mb-1">AI Model Migrator</h2>
            <p className="text-gray-600 text-center">Converts data models and calculations to Power BI semantic models and DAX.</p>
          </div>

          {/* Arrow */}
          <div className="w-fit h-fit flex justify-center items-center">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-7 w-7 text-blue-500 hidden md:block animate-slide-right"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2.5}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M4 12h16m-6-6l6 6-6 6" />
            </svg>

            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-7 w-7 text-blue-500 block md:hidden animate-slide-down"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2.5}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m6-6l-6 6-6-6" />
            </svg>
          </div>

          {/* Box 3 */}
          <div className="bg-gradient-to-br from-gray-50 to-gray-100 rounded-2xl shadow p-6 flex flex-col items-center h-40 min-w-[260px] w-full md:w-80 max-w-xs flex-shrink-0">
            <span className="bg-gray-700 text-white rounded-full mb-3">
              <svg width="32" height="32" fill="none" viewBox="0 0 32 32">
                <circle cx="16" cy="16" r="16" fill="#374151" />
                <rect x="10" y="18" width="2" height="4" rx="1" fill="#fff" />
                <rect x="14" y="14" width="2" height="8" rx="1" fill="#fff" />
                <rect x="18" y="10" width="2" height="12" rx="1" fill="#fff" />
                <rect x="22" y="20" width="2" height="2" rx="1" fill="#fff" />
              </svg>
            </span>
            <h2 className="text-xl font-bold text-gray-700 mb-1">AI Visual Converter</h2>
            <p className="text-gray-600 text-center">Maps Tableau visuals to Power BI, preserving logic and visual integrity.</p>
          </div>
        </div>


        <div className="flex justify-center w-full">
          <button
            className="bg-gradient-to-r from-blue-600 to-orange-500 text-white px-8 py-4 rounded-2xl font-semibold hover:bg-blue-700 shadow-xl transition-all transform hover:-translate-y-1 hover:scale-105 text-xl tracking-wide cursor-pointer max-w-full"
            onClick={() => onNext("DefaultCompany", "DefaultProject")}
          >
            🚀 Launch Migration
          </button>
        </div>
      </div>
    </div>
  );
}
