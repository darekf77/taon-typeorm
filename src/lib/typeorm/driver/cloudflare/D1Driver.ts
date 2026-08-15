import { DataSource } from '../../data-source/DataSource';
import { AbstractSqliteDriver } from '../sqlite-abstract/AbstractSqliteDriver';
import { ReplicationMode } from '../types/ReplicationMode';

import { D1QueryRunner } from './D1QueryRunner';
export type D1Database = any;

export class D1Driver extends AbstractSqliteDriver {
  declare readonly databaseConnection: D1Database;

  constructor(connection: DataSource, database: D1Database) {
    super(connection);

    this.databaseConnection = database;
  }

  async connect(): Promise<void> {
    // D1 binding is already connected.
  }

  async disconnect(): Promise<void> {
    // Nothing to close.
  }

  createQueryRunner(mode: ReplicationMode) {
    return new D1QueryRunner(this);
  }

  async createDatabaseConnection() {
    return this.databaseConnection;
  }
}
