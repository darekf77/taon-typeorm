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

    const views: Array<{ name: string }> = await this.query(`
      SELECT name
      FROM sqlite_master
      WHERE type = 'view'
        AND name NOT LIKE '_cf_%'
        AND name NOT LIKE 'sqlite_%'
    `);

    const tables: Array<{ name: string }> = await this.query(`
      SELECT name
      FROM sqlite_master
      WHERE type = 'table'
        AND name NOT LIKE '_cf_%'
        AND name NOT LIKE 'sqlite_%'
    `);

    // Views first
    for (const { name } of views) {
      const sql = `DROP VIEW IF EXISTS "${this.escapeIdentifier(name)}"`;
      console.log('[D1 DROP]', sql);
      await this.query(sql);
    }

    const dependencies = new Map<string, Set<string>>();

    for (const { name } of tables) {
      const escapedName = this.escapeIdentifier(name);

      const foreignKeys: Array<{ table: string }> = await this.query(
        `PRAGMA foreign_key_list("${escapedName}")`,
      );

      dependencies.set(
        name,
        new Set(
          foreignKeys
            .map(x => x.table)
            .filter(parent => tables.some(table => table.name === parent)),
        ),
      );
    }

    const remaining = new Set(tables.map(x => x.name));

    while (remaining.size > 0) {
      // Drop tables which are NOT parents of another remaining table.
      // In other words: FK children first.
      const candidates = [...remaining].filter(tableName => {
        return ![...remaining].some(otherTable => {
          if (otherTable === tableName) {
            return false;
          }

          return dependencies.get(otherTable)?.has(tableName);
        });
      });

      // Cyclic FK graph.
      if (candidates.length === 0) {
        throw new Error(
          `[D1] Cannot clear database because of cyclic foreign keys: ` +
            [...remaining].join(', '),
        );
      }

      for (const name of candidates) {
        const sql = `DROP TABLE IF EXISTS "${this.escapeIdentifier(name)}"`;

        console.log('[D1 DROP]', sql);

        await this.query(sql);

        remaining.delete(name);
      }
    }
  }

  private escapeIdentifier(name: string): string {
    return name.replace(/"/g, '""');
  }

  private isInternalD1Object(name: string): boolean {
    return name.startsWith('_cf_') || name.startsWith('sqlite_');
  }

  async query(
    query: string,
    parameters: any[] = [],
    useStructuredResult = false,
  ): Promise<any> {
    // console.log('[D1 QUERY]', query);
    // console.log('[D1 PARAMS]', parameters);

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

    const sqliteLikeResult = {
      ...result,
      lastID: result.meta?.last_row_id,
      changes: result.meta?.changes,
    };

    if (!useStructuredResult) {
      return sqliteLikeResult;
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
