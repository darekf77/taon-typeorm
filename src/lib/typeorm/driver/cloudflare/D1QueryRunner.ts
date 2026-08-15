import { QueryResult } from '../../query-runner/QueryResult';
import { Broadcaster } from '../../subscriber/Broadcaster';
import { AbstractSqliteQueryRunner } from '../sqlite-abstract/AbstractSqliteQueryRunner';

import { D1Driver } from './D1Driver';

let queryRunnerCounter = 0;
let queryCounter = 0;

export class D1QueryRunner extends AbstractSqliteQueryRunner {
  private readonly debugId = ++queryRunnerCounter;

  constructor(public driver: D1Driver) {
    super();
    this.driver = driver;
    this.connection = driver.connection;
    this.broadcaster = new Broadcaster(this);
    // console.log('[D1 QR CONSTRUCTOR]', {
    //   driver: !!this.driver,
    //   connection: !!this.connection,
    //   broadcaster: !!this.broadcaster,
    //   databaseConnection: !!this.driver.databaseConnection,
    //   transactionSupport: this.driver.transactionSupport,
    // });

    // console.log(`[D1 QR#${this.debugId}] CREATED`);
  }

  async clearDatabase(): Promise<void> {
    console.log('[D1] clearDatabase');

    const views: Array<{ query: string }> = await this.query(`
      SELECT 'DROP VIEW "' || name || '";' AS query
      FROM sqlite_master
      WHERE type = 'view'
        AND name NOT LIKE '_cf_%'
        AND name NOT LIKE 'sqlite_%'
    `);

    const tables: Array<{ query: string }> = await this.query(`
      SELECT 'DROP TABLE "' || name || '";' AS query
      FROM sqlite_master
      WHERE type = 'table'
        AND name NOT LIKE '_cf_%'
        AND name NOT LIKE 'sqlite_%'
    `);

    for (const { query } of views) {
      console.log('[D1 DROP]', query);
      await this.query(query);
    }

    for (const { query } of tables) {
      console.log('[D1 DROP]', query);
      await this.query(query);
    }
  }

  private isInternalD1Object(name: string): boolean {
    return name.startsWith('_cf_') || name.startsWith('sqlite_');
  }

  async query(
    query: string,
    parameters: any[] = [],
    useStructuredResult = false,
  ): Promise<any> {
    const db = this.driver.databaseConnection;

    let stmt = db.prepare(query);

    if (parameters.length > 0) {
      stmt = stmt.bind(...parameters);
    }

    const normalized = query.trim().toUpperCase();

    const isRead =
      normalized.startsWith('SELECT') ||
      normalized.startsWith('WITH') ||
      (normalized.startsWith('PRAGMA') && !normalized.includes('='));

    if (isRead) {
      const result = await stmt.all();

      if (!useStructuredResult) {
        return result.results ?? [];
      }

      const qr = new QueryResult();
      qr.records = result.results ?? [];
      qr.raw = result;

      return qr;
    }

    const result = await stmt.run();

    if (!useStructuredResult) {
      return result;
    }

    const qr = new QueryResult();
    qr.raw = result;
    qr.affected = result.meta?.changes ?? 0;

    return qr;
  }

  async startTransaction() {
    this.isTransactionActive = true;
    this.transactionDepth++;
  }

  async commitTransaction() {
    this.transactionDepth = Math.max(0, this.transactionDepth - 1);
    this.isTransactionActive = this.transactionDepth > 0;
  }

  async rollbackTransaction() {
    this.transactionDepth = Math.max(0, this.transactionDepth - 1);
    this.isTransactionActive = this.transactionDepth > 0;
  }

  // private debug(label: string, extra: any = {}) {
  //   console.log(`[D1 QR] ${label}`, {
  //     isReleased: this.isReleased,
  //     isTransactionActive: this.isTransactionActive,
  //     transactionDepth: (this as any).transactionDepth,
  //     hasDriver: !!this.driver,
  //     hasConnection: !!this.connection,
  //     hasBroadcaster: !!this.broadcaster,
  //     hasDatabaseConnection: !!this.driver?.databaseConnection,
  //     ...extra,
  //   });
  // }
}

// async function debugPromise<T = any>(
//   name: string,
//   promise: Promise<T>,
//   timeoutMs = 3000,
// ): Promise<T> {
//   const timeout = new Promise<never>((_, reject) => {
//     setTimeout(() => {
//       reject(new Error(`[D1 TIMEOUT] ${name} after ${timeoutMs}ms`));
//     }, timeoutMs);
//   });

//   return Promise.race([promise, timeout]);
// }
