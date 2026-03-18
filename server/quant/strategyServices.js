import { StrategyParser } from './strategyParser.js';

const MAX_FILE_SIZE_BYTES = 1024 * 1024;

export class StrategyValidationService {
  validateUpload({ fileName, content }) {
    if (!fileName || !content) {
      return { valid: false, errors: ['Strategy file name and content are required.'] };
    }

    if (Buffer.byteLength(content, 'utf8') > MAX_FILE_SIZE_BYTES) {
      return { valid: false, errors: ['Strategy file exceeds 1 MB limit.'] };
    }

    const extension = fileName.split('.').at(-1)?.toLowerCase();
    if (extension !== 'json') {
      return { valid: false, errors: ['Only JSON strategy files are supported in v1.'] };
    }

    return { valid: true, errors: [] };
  }
}

export class StrategyUploadService {
  constructor({ validationService, parserService = new StrategyParser(), saveStrategyRecord }) {
    this.validationService = validationService;
    this.parserService = parserService;
    this.saveStrategyRecord = saveStrategyRecord;
  }

  handleUpload(payload) {
    const validation = this.validationService.validateUpload(payload);
    if (!validation.valid) {
      return { status: 'invalid', validation, strategy: null };
    }

    const parseResult = this.parserService.parse(payload.content);
    if (!parseResult.valid) {
      return {
        status: 'invalid',
        validation: { valid: false, errors: parseResult.errors },
        parseResult,
        strategy: null
      };
    }

    const strategy = this.saveStrategyRecord({
      file_name: payload.fileName,
      raw_content: payload.content,
      parse_status: 'parsed',
      metadata_json: JSON.stringify(parseResult.summary || {}),
      parse_message: 'Validated against strict v1 schema.'
    });

    return {
      status: 'parsed',
      validation,
      parseResult,
      strategy
    };
  }
}
