import type { NextApiRequest, NextApiResponse } from 'next';
import fs from 'fs';
import path from 'path';
import yaml from 'js-yaml';

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  const configPath = path.resolve(process.cwd(), 'config', 'config.yaml');
  try {
    const file = fs.readFileSync(configPath, 'utf8');
    const config = yaml.load(file) as unknown;
    let output_path = '';
    let type = '';
    let name = '';
    if (typeof config === 'object' && config !== null) {
      const c = config as { paths?: { output_path?: string }, report?: { type?: string, name?: string } };
      output_path = c.paths?.output_path || '';
      type = c.report?.type || '';
      name = c.report?.name || '';
    }
    res.status(200).json({ output_path, type, name });
  } catch (err: unknown) {
    if (err instanceof Error) {
      res.status(500).json({ error: err.message });
    } else {
      res.status(500).json({ error: 'Unknown error' });
    }
  }
}
