import { ObjectLiteral } from '../../common/ObjectLiteral';
import { DataSource } from '../../data-source/DataSource';
import { EntityMetadata } from '../../metadata/EntityMetadata';
import { ReturningType } from '../Driver';
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

  isReturningSqlSupported(returningType?: ReturningType): boolean {
    return false;
  }

  createQueryRunner(mode: ReplicationMode) {
    return new D1QueryRunner(this);
  }

  async createDatabaseConnection() {
    return this.databaseConnection;
  }

  createGeneratedMap(
    metadata: EntityMetadata,
    insertResult: any,
    entityIndex: number,
  ): ObjectLiteral | undefined {
    // console.log('[D1 createGeneratedMap]', {
    //   insertResult,
    //   generatedColumns: metadata.generatedColumns.map(c => ({
    //     propertyName: c.propertyName,
    //     generationStrategy: c.generationStrategy,
    //     type: c.type,
    //   })),
    // });

    const generatedColumn = metadata.generatedColumns.find(
      column => column.isPrimary && column.generationStrategy === 'increment',
    );

    if (!generatedColumn) {
      return undefined;
    }

    const value = insertResult?.meta?.last_row_id ?? insertResult?.lastID;

    if (value === undefined || value === null) {
      return undefined;
    }

    return generatedColumn.createValueMap(value);
  }
}
