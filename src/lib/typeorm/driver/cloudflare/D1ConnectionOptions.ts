import { BaseDataSourceOptions } from '../../data-source/BaseDataSourceOptions';

import { D1Database } from './D1Driver';

// @ts-ignore
export interface D1ConnectionOptions extends BaseDataSourceOptions {
  readonly type: 'd1';

  readonly database: D1Database;
}
