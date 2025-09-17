import type { NextApiRequest, NextApiResponse } from 'next';
import fs from 'fs';
import path from 'path';
import yaml from 'js-yaml';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    res.status(405).json({ error: 'Method not allowed' });
    return;
  }
  const { fileName } = req.body;
  try {
    const configPath = path.resolve(process.cwd(), 'config', 'config.yaml');
    const file = fs.readFileSync(configPath, 'utf8');
    const config = yaml.load(file) as unknown;
    let log_path = '';
    let report_type = '';
    if (typeof config === 'object' && config !== null) {
      const c = config as { paths?: { log_path?: string }, report?: { type?: string } };
      log_path = c.paths?.log_path || '';
      report_type = c.report?.type || '';
    }
    // Use today's date for log folder
    const today = new Date().toISOString().slice(0, 10);
    const report_name = fileName.replace(/\.twb$/, '');
    const log_dir = path.join(log_path, report_type, today, report_name);
    if (fs.existsSync(log_dir)) {
      fs.rmSync(log_dir, { recursive: true, force: true });
    }
    res.status(200).json({ status: 'success' });
  } catch (err: unknown) {
    if (err instanceof Error) {
      res.status(500).json({ error: err.message });
    } else {
      res.status(500).json({ error: 'Unknown error' });
    }
  }
}
