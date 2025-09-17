import { NextRequest } from 'next/server';
import { spawn } from 'child_process';
import path from 'path';

export async function POST(req: NextRequest): Promise<Response> {
  const data = await req.json();
  const folderName = data.folderName;

  // Path to the Python script
  const scriptPath = path.resolve(process.cwd(), 'python_scripts', 'generate_metadata.py');

  // Pass folderName as an argument to the script if needed
  // If your script expects the folder as an argument, update the Python code accordingly
  return await new Promise<Response>((resolve) => {
    const pyProcess = spawn('python', [scriptPath, folderName]);
    let output = '';
    let error = '';

    pyProcess.stdout.on('data', (data) => {
      output += data.toString();
    });
    pyProcess.stderr.on('data', (data) => {
      error += data.toString();
    });
    pyProcess.on('close', (code) => {
      if (code === 0) {
        resolve(new Response(JSON.stringify({ success: true, output }), { status: 200 }));
      } else {
        resolve(new Response(JSON.stringify({ success: false, error }), { status: 500 }));
      }
    });
  });
}
