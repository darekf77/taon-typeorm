import { LoggerOptions } from "./LoggerOptions"
import { PlatformTools } from "../platform/PlatformTools"
import { QueryRunner } from "../query-runner/QueryRunner"
import { Logger } from "./Logger"
import { GlobalStorage } from "tnp-core/src";

/**
 * Performs logging of the events in TypeORM.
 * This version of logger uses console to log events and use syntax highlighting.
 */
export class AdvancedConsoleLogger implements Logger {
    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(private options?: LoggerOptions) {}

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Logs query and parameters used in it.
     */
    logQuery(
      query: string,
      parameters?: any[],
      queryRunner?: QueryRunner,
    ) {
      if (
        this.options === 'all' ||
        this.options === true ||
        (Array.isArray(this.options) &&
          this.options.includes('query'))
      ) {

        const DISPLAY_RAW_SQL_COMMANDS = GlobalStorage.get('DISPLAY_RAW_SQL_COMMANDS') === 'true';
        // Replace with your actual custom D1 driver detection.

        if (DISPLAY_RAW_SQL_COMMANDS) {
          const sql = interpolateSqliteQuery(query, parameters);

          PlatformTools.logInfo('query:', sql);
          return;
        }

        const sql =
          query +
          (parameters?.length
            ? ' -- PARAMETERS: ' + this.stringifyParams(parameters)
            : '');

        PlatformTools.logInfo(
          'query:',
          PlatformTools.highlightSql(sql),
        );
      }
    }

    /**
     * Logs query that is failed.
     */
    logQueryError(
        error: string,
        query: string,
        parameters?: any[],
        queryRunner?: QueryRunner,
    ) {
        if (
            this.options === "all" ||
            this.options === true ||
            (Array.isArray(this.options) &&
                this.options.indexOf("error") !== -1)
        ) {
            const sql =
                query +
                (parameters && parameters.length
                    ? " -- PARAMETERS: " + this.stringifyParams(parameters)
                    : "")
            PlatformTools.logError(
                `query failed:`,
                PlatformTools.highlightSql(sql),
            )
            PlatformTools.logError(`error:`, error)
        }
    }

    /**
     * Logs query that is slow.
     */
    logQuerySlow(
        time: number,
        query: string,
        parameters?: any[],
        queryRunner?: QueryRunner,
    ) {
        const sql =
            query +
            (parameters && parameters.length
                ? " -- PARAMETERS: " + this.stringifyParams(parameters)
                : "")
        PlatformTools.logWarn(`query is slow:`, PlatformTools.highlightSql(sql))
        PlatformTools.logWarn(`execution time:`, time)
    }

    /**
     * Logs events from the schema build process.
     */
    logSchemaBuild(message: string, queryRunner?: QueryRunner) {
        if (
            this.options === "all" ||
            (Array.isArray(this.options) &&
                this.options.indexOf("schema") !== -1)
        ) {
            PlatformTools.log(message)
        }
    }

    /**
     * Logs events from the migration run process.
     */
    logMigration(message: string, queryRunner?: QueryRunner) {
        PlatformTools.log(message)
    }

    /**
     * Perform logging using given logger, or by default to the console.
     * Log has its own level and message.
     */
    log(
        level: "log" | "info" | "warn",
        message: any,
        queryRunner?: QueryRunner,
    ) {
        switch (level) {
            case "log":
                if (
                    this.options === "all" ||
                    (Array.isArray(this.options) &&
                        this.options.indexOf("log") !== -1)
                )
                    PlatformTools.log(message)
                break
            case "info":
                if (
                    this.options === "all" ||
                    (Array.isArray(this.options) &&
                        this.options.indexOf("info") !== -1)
                )
                    PlatformTools.logInfo("INFO:", message)
                break
            case "warn":
                if (
                    this.options === "all" ||
                    (Array.isArray(this.options) &&
                        this.options.indexOf("warn") !== -1)
                )
                    console.warn(PlatformTools.warn(message))
                break
        }
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Converts parameters to a string.
     * Sometimes parameters can have circular objects and therefore we are handle this case too.
     */
    protected stringifyParams(parameters: any[]) {
        try {
            return JSON.stringify(parameters)
        } catch (error) {
            // most probably circular objects in parameters
            return parameters
        }
    }
}

 export function interpolateSqliteQuery(
  query: string,
  parameters: unknown[] = [],
): string {
  const toLiteral = (value: unknown): string => {
    if (value === null) return 'NULL';

    if (value === undefined) {
      throw new Error('Undefined SQL parameter');
    }

    if (typeof value === 'string') {
      return `'${value.replace(/'/g, "''")}'`;
    }

    if (typeof value === 'number') {
      if (!Number.isFinite(value)) {
        throw new Error(`Invalid SQL number: ${value}`);
      }
      return String(value);
    }

    if (typeof value === 'bigint') {
      return value.toString();
    }

    if (typeof value === 'boolean') {
      return value ? '1' : '0';
    }

    if (value instanceof Date) {
      return `'${value.toISOString().replace(/'/g, "''")}'`;
    }

    if (value instanceof Uint8Array) {
      const hex = Array.from(value)
        .map(byte => byte.toString(16).padStart(2, '0'))
        .join('');

      return `X'${hex}'`;
    }

    throw new Error(
      `Unsupported SQLite parameter type: ${typeof value}`,
    );
  };

  type State =
    | 'normal'
    | 'single'
    | 'double'
    | 'backtick'
    | 'bracket'
    | 'line-comment'
    | 'block-comment';

  let state: State = 'normal';
  let result = '';

  // Next SQLite positional parameter index (1-based).
  let nextParameterIndex = 1;

  // Highest parameter index assigned so far.
  let maxParameterIndex = 0;

  const usedParameters = new Set<number>();

  const appendParameter = (index: number): void => {
    if (
      !Number.isSafeInteger(index) ||
      index < 1 ||
      index > parameters.length
    ) {
      throw new Error(`Missing SQL parameter: ${index}`);
    }

    usedParameters.add(index - 1);
    result += toLiteral(parameters[index - 1]);
  };

  for (let i = 0; i < query.length; i++) {
    const char = query[i];
    const next = query[i + 1];

    if (state === 'normal') {
      if (char === "'") {
        state = 'single';
      } else if (char === '"') {
        state = 'double';
      } else if (char === '`') {
        state = 'backtick';
      } else if (char === '[') {
        state = 'bracket';
      } else if (char === '-' && next === '-') {
        state = 'line-comment';
      } else if (char === '/' && next === '*') {
        state = 'block-comment';
      } else if (char === '?' || char === '$') {
        const remaining = query.slice(i);

        const match =
          char === '?'
            ? remaining.match(/^\?(\d+)/)
            : remaining.match(/^\$(\d+)/);

        if (match) {
          const index = Number(match[1]);

          maxParameterIndex = Math.max(maxParameterIndex, index);
          nextParameterIndex = maxParameterIndex + 1;

          appendParameter(index);

          i += match[0].length - 1;
          continue;
        }

        if (char === '?') {
          const index = nextParameterIndex++;

          maxParameterIndex = Math.max(maxParameterIndex, index);

          appendParameter(index);
          continue;
        }
      }
    } else if (state === 'single' && char === "'") {
      if (next === "'") {
        result += "''";
        i++;
        continue;
      }

      state = 'normal';
    } else if (state === 'double' && char === '"') {
      if (next === '"') {
        result += '""';
        i++;
        continue;
      }

      state = 'normal';
    } else if (state === 'backtick' && char === '`') {
      if (next === '`') {
        result += '``';
        i++;
        continue;
      }

      state = 'normal';
    } else if (state === 'bracket' && char === ']') {
      state = 'normal';
    } else if (state === 'line-comment' && char === '\n') {
      state = 'normal';
    } else if (
      state === 'block-comment' &&
      char === '*' &&
      next === '/'
    ) {
      result += '*/';
      i++;
      state = 'normal';
      continue;
    }

    result += char;
  }

  if (usedParameters.size !== parameters.length) {
    const unused = parameters
      .map((_, index) => index)
      .filter(index => !usedParameters.has(index));

    throw new Error(
      `Unused SQL parameters: ${unused.map(i => i + 1).join(', ')}`,
    );
  }

  return result;
}
