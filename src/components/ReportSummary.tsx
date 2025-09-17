"use client";


export default function ReportSummary() {


  // Example Power BI embed URL (replace with your actual report URL)
  const powerBIUrl = "https://app.powerbi.com/reportEmbed?reportId=d99be48e-565d-4e8c-89a7-e95d279c1359&autoAuth=true&ctid=bdcfaa46-3f69-4dfd-b3f7-c582bdfbb820&navContentPaneEnabled=false";

  return (
  <div className="flex-1 min-h-full flex flex-col items-center justify-start bg-gradient-to-br from-blue-100 via-orange-50 to-gray-100 font-[Calibri]">
      <div className="w-[90%] bg-white shadow-2xl rounded-3xl p-2 mt-5 border from-blue-100 via-orange-50 to-gray-100 font-[Calibri] border-gray-200 overflow-x-auto overflow-y-visible">
        <div className="w-full" style={{ aspectRatio: '16/9', maxHeight: '73vh' }}>
          <iframe
            title="Power BI Report"
            src={powerBIUrl}
            width="100%"
            height="100%"
            style={{ border: "none", borderRadius: "8px", width: '100%', height: '100%' }}
            allowFullScreen
          />
        </div>
      </div>
    </div>
  );
}
