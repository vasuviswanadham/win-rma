import type { NextApiRequest, NextApiResponse } from 'next';
import fs from 'fs';
import path from 'path';
import yaml from 'js-yaml';

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  // Read config.yaml for dynamic path
  const configPath = path.resolve(process.cwd(), 'config', 'config.yaml');
  let logPath = '';
  try {
    const file = fs.readFileSync(configPath, 'utf8');
    const config = yaml.load(file) as unknown;
    let output_path = '';
    let type = '';
    let name = '';
    if (typeof config === 'object' && config !== null) {
      const c = config as { paths?: { log_path?: string }, report?: { type?: string, name?: string } };
      output_path = c.paths?.log_path || '';
      type = c.report?.type || '';
      name = c.report?.name || '';
    }
    // Use today's date for log file name
    const now = new Date();
    const localISO = new Date(now.getTime() - now.getTimezoneOffset() * 60000)
      .toISOString()
      .slice(0, 10);
    console.log(localISO); // YYYY-MM-DD in local time
    const today = localISO

    // const today = new Date().toISOString().slice(0, 10);
    logPath = path.resolve(`${output_path}/${type}/${today}/${name}`, `summary_${today}.log`);
  } catch (err: unknown) {
    if (err instanceof Error) {
      res.status(500).send('Error reading config.yaml: ' + err.message);
    } else {
      res.status(500).send('Error reading config.yaml: Unknown error');
    }
    return;
  }

  // Path to the summary log file
  try {
    console.log('Summary log path:', logPath);
    if (!fs.existsSync(logPath)) {
      res.status(404).send('Summary log not found at: ' + logPath);
      return;
    }
    const logContent = fs.readFileSync(logPath, 'utf-8');
    if (!logContent.trim()) {
      res.status(200).send('Summary log is empty.');
      return;
    }
    res.status(200).send(logContent);
  } catch (err: unknown) {
    if (err instanceof Error) {
      res.status(500).send('Error reading summary log: ' + err.message);
    } else {
      res.status(500).send('Error reading summary log: Unknown error');
    }
  }
}
