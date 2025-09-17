"use client";
import { useState, useRef, useEffect } from "react";

export default function ReportMigrator({}: { onBack?: () => void }) {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [progress, setProgress] = useState(0); // target progress
  const [displayProgress, setDisplayProgress] = useState(0); // animated text progress
  const [logs, setLogs] = useState<string[]>([]);
  const [summaryLogs, setSummaryLogs] = useState<string>("");
  const [outputPath, setOutputPath] = useState("");

  // Smooth percentage text animation synced with CSS bar
  useEffect(() => {
    if (progress === displayProgress) return;

    const duration = 1000; // match CSS transition duration (ms)
    const diff = Math.abs(progress - displayProgress);
    const stepTime = duration / diff; // time per increment/decrement

    const step = progress > displayProgress ? 1 : -1;
    const interval = setInterval(() => {
      setDisplayProgress((prev) => {
        if (
          (step > 0 && prev >= progress) ||
          (step < 0 && prev <= progress)
        ) {
          clearInterval(interval);
          return progress;
        }
        return prev + step;
      });
    }, stepTime);

    return () => clearInterval(interval);
  }, [progress, displayProgress]);

  // Progress calculation based on log steps
  useEffect(() => {
    const stepPercentages: { [key: string]: number } = {
      "Migration started...": 10,
      "Semantic Model Migration process started": 20,
      "Semantic Model Migration process completed": 35,
      "Visual Extraction process started": 40,
      "Visual Extraction process completed": 65,
      "Visual Migration process started": 70,
      "Visual Migration process completed": 95,
      "Migration completed": 100,
    };
    let maxPercent = 0;
    const logLines = summaryLogs.split("\n").map((line) => line.trim());
    for (const step in stepPercentages) {
      if (logLines.some((line) => line.includes(step))) {
        maxPercent = Math.max(maxPercent, stepPercentages[step]);
      }
    }
    if (maxPercent > progress) {
      setProgress(maxPercent);
    }
  }, [summaryLogs]);

  // Poll summary log only while migration is in progress
  useEffect(() => {
    let interval: NodeJS.Timeout | null = null;
    const fetchSummaryLog = async () => {
      try {
        const response = await fetch("/api/summary-log");
        if (response.ok) {
          const text = await response.text();
          setSummaryLogs(text);
        } else {
          setSummaryLogs("Could not load summary log.");
        }
      } catch (err) {
        setSummaryLogs("Error loading summary log.");
      }
    };
    if (isLoading && progress < 100) {
      fetchSummaryLog();
      interval = setInterval(fetchSummaryLog, 3000);
    } else if (isLoading && progress === 100) {
      // Migration just finished, fetch logs one last time and stop polling
      fetchSummaryLog();
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [isLoading, progress]);

  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileSelect = async (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    const file = event.target.files?.[0];
    if (file) {
      // Update config and cleanup via backend
      await fetch("http://localhost:5000/update-config", {
        method: "POST",
        body: (() => {
          const formData = new FormData();
          formData.append("fileName", file.name);
          return formData;
        })(),
      });
      setSelectedFile(file);
      setProgress(0); // Reset progress when new file is selected
      setDisplayProgress(0);
      setLogs([]);
      setSummaryLogs(""); // Clear summary logs when new file is selected
    }
  };

  const addLog = (message: string) => {
    const timestamp = new Date().toLocaleTimeString();
    setLogs((prev) => [...prev, `[${timestamp}] ${message}`]);
  };

  // Call Flask API -> run_pipeline()
  const handleRunPipeline = async () => {
    if (!selectedFile) return;

    setIsLoading(true);
    setProgress(0); // Reset progress when migration starts
    setDisplayProgress(0);
    setLogs([]);
    setSummaryLogs(""); // Clear summary logs when migration starts

    try {
      const formData = new FormData();
      formData.append("fileName", selectedFile.name);

      const response = await fetch("http://localhost:5000/run-pipeline", {
        method: "POST",
        body: formData,
      });

      const data = await response.json();

      if (data.status !== "success") {
        addLog("❌ Pipeline failed: " + (data.message || "Unknown error"));
      }
    } catch (err: unknown) {
      if (err instanceof Error) {
        addLog("❌ Error calling backend: " + err.message);
      } else {
        addLog("❌ Error calling backend: Unknown error");
      }
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetch("/api/config-values")
      .then((res) => res.json())
      .then((data) => {
        if (data.output_path && data.type && data.name) {
          setOutputPath(`${data.output_path}/${data.type}/${data.name}`);
        }
      });
  }, []);

  useEffect(() => {
    if (!isLoading && progress === 100) {
      setSelectedFile(null); // Clear selected file after migration completes
      if (fileInputRef.current) {
        fileInputRef.current.value = ""; // Reset file input
      }
    }
  }, [isLoading, progress]);

  return (
    <div className="flex-1 min-h-full flex flex-col items-center justify-start bg-gradient-to-br from-blue-100 via-orange-50 to-gray-100 font-[Calibri]">
      <div className="w-[80%] bg-white shadow-2xl rounded-3xl p-8 mt-5 border from-blue-100 via-orange-50 to-gray-100 font-[Calibri] border-gray-200 overflow-x-auto overflow-y-visible">
        {/* File Selector */}
        <div className="w-full mb-2">
          <label className="block font-medium mb-2 text-gray-700">
            Select Report File
          </label>
          <input
            ref={fileInputRef}
            type="file"
            onChange={handleFileSelect}
            accept=".twb,.xml"
            className="w-full border border-gray-300 px-3 py-2 rounded-lg text-base text-gray-700 cursor-pointer file:border-0 file:bg-gray-100 file:px-4 file:py-2 file:rounded-lg file:text-gray-700 file:cursor-pointer file:hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500"
            disabled={isLoading}
          />

          {selectedFile && (
            <p className="mt-2 text-sm text-gray-600">
              Selected: {selectedFile.name} (
              {Math.round(selectedFile.size / 1024)} KB)
            </p>
          )}
        </div>

        {/* Migrate Button */}
        {selectedFile && (
          <div className="w-full mb-6 flex justify-center">
            <button
              onClick={handleRunPipeline}
              disabled={isLoading}
              // className="bg-gradient-to-r from-blue-600 to-orange-500 text-white px-8 py-4 rounded-2xl font-semibold hover:bg-blue-700 shadow-xl transition-all transform hover:-translate-y-1 hover:scale-105 text-xl tracking-wide cursor-pointer max-w-full"
              className={`px-8 py-3 rounded-2xl text-lg font-semibold shadow-lg transition-all transform ${isLoading
                ? "bg-gradient-to-r from-blue-600 to-orange-500  text-white cursor-not-allowed"
                : "bg-gradient-to-r from-blue-600 to-orange-500 hover:scale-105 text-white hover:bg-blue-700 active:bg-green-600 cursor-pointer"
                }`}
            >
              {isLoading ? "Migrating..." : "Start Migration"}
            </button>
          </div>
        )}

        {/* Success Message and Output Path after migration */}
        {!isLoading && progress == 100 && (
          <div className="w-full mb-6 flex flex-col items-center">
            <span className="text-green-700 font-semibold text-lg mb-2">
              Migration completed successfully!
            </span>
            <div className="flex items-center gap-2">
              <span className="text-base text-gray-700 font-mono bg-gray-100 px-2 py-1 rounded">
                {outputPath}
              </span>
              <button
                onClick={() => navigator.clipboard.writeText(outputPath)}
                className="px-3 py-1 rounded bg-blue-600 text-white text-sm font-semibold hover:bg-blue-700 active:bg-green-600"
              >
                Copy to clipboard
              </button>
            </div>
          </div>
        )}

        {/* Progress Bar */}
        {isLoading && (
          <div className="w-full mb-6">
            <label className="block font-medium mb-2 text-gray-700">
              Migration Progress
            </label>
            <div className="w-full h-5 bg-gray-200 rounded-full overflow-hidden">
              <div
                className="h-5 rounded-full bg-gradient-to-r from-blue-400 via-blue-500 to-blue-600 transition-[width] duration-1000 ease-in-out"
                style={{ width: `${progress}%` }}
              />
            </div>
            <p className="text-center mt-2 text-sm text-gray-600">
              {displayProgress}% Complete
            </p>
          </div>
        )}

        {/* Logs */}
        {(logs.length > 0 || summaryLogs) && (
          <div className="w-full">
            <label className="block font-medium mb-2 text-gray-700">
              Migration Logs
            </label>
            <textarea
              value={[summaryLogs, ...logs].filter(Boolean).join("\n")}
              readOnly
              className="w-full border border-gray-300 px-4 py-3 rounded-lg min-h-[250px] text-sm font-mono bg-gray-50 shadow-inner text-black"
              style={{ resize: "vertical" }}
            />
          </div>
        )}
      </div>
    </div>
  );
}
