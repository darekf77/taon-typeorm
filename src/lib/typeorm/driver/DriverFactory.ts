import { MissingDriverError } from "../error/MissingDriverError"
//#region @backend
import { CockroachDriver } from "./cockroachdb/CockroachDriver"
import { MongoDriver } from "./mongodb/MongoDriver"
import { SqlServerDriver } from "./sqlserver/SqlServerDriver"
import { OracleDriver } from "./oracle/OracleDriver"
import { SqliteDriver } from "./sqlite/SqliteDriver"
//#endregion
import { CordovaDriver } from "./cordova/CordovaDriver"
import { ReactNativeDriver } from "./react-native/ReactNativeDriver"
import { NativescriptDriver } from "./nativescript/NativescriptDriver"
import { SqljsDriver } from "./sqljs/SqljsDriver"
//#region @backend
import { MysqlDriver } from "./mysql/MysqlDriver"
import { PostgresDriver } from "./postgres/PostgresDriver"
import { ExpoDriver } from "./expo/ExpoDriver"
import { AuroraMysqlDriver } from "./aurora-mysql/AuroraMysqlDriver"
import { AuroraPostgresDriver } from "./aurora-postgres/AuroraPostgresDriver"
//#endregion
import { Driver } from "./Driver"
import { DataSource } from "../data-source/DataSource"
//#region @backend
import { SapDriver } from "./sap/SapDriver"
import { BetterSqlite3Driver } from "./better-sqlite3/BetterSqlite3Driver"
import { CapacitorDriver } from "./capacitor/CapacitorDriver"
import { SpannerDriver } from "./spanner/SpannerDriver"
//#endregion


/**
 * Helps to create drivers.
 */
export class DriverFactory {
    /**
     * Creates a new driver depend on a given connection's driver type.
     */
    create(connection: DataSource): Driver {
        const { type } = connection.options
        switch (type) {
          //#region @backend
            case "mysql":
                return new MysqlDriver(connection)
            case "postgres":
                return new PostgresDriver(connection)
            case "cockroachdb":
                return new CockroachDriver(connection)
            case "sap":
                return new SapDriver(connection)
            case "mariadb":
                return new MysqlDriver(connection)
            case "sqlite":
                return new SqliteDriver(connection)
            case "better-sqlite3":
                return new BetterSqlite3Driver(connection)
            //#endregion
            case "cordova":
                return new CordovaDriver(connection)
            case "nativescript":
                return new NativescriptDriver(connection)
            case "react-native":
                return new ReactNativeDriver(connection)
            case "sqljs":
                return new SqljsDriver(connection)
            //#region @backend
            case "oracle":
                return new OracleDriver(connection)
            case "mssql":
                return new SqlServerDriver(connection)
            case "mongodb":
                return new MongoDriver(connection)
            case "expo":
                return new ExpoDriver(connection)
            case "aurora-mysql":
                return new AuroraMysqlDriver(connection)
            case "aurora-postgres":
                return new AuroraPostgresDriver(connection)
            case "capacitor":
                return new CapacitorDriver(connection)
            case "spanner":
                return new SpannerDriver(connection)
            //#endregion
            default:
                throw new MissingDriverError(type, [
                  //#region @backend
                    "aurora-mysql",
                    "aurora-postgres",
                    "better-sqlite3",
                    "capacitor",
                    "cockroachdb",
                    //#endregion
                    "cordova",
                    //#region @backend
                    "expo",
                    "mariadb",
                    "mongodb",
                    "mssql",
                    "mysql",
                    //#endregion
                    "nativescript",
                    //#region @backend
                    "oracle",
                    "postgres",
                    //#endregion
                    "react-native",
                    //#region @backend
                    "sap",
                    "sqlite",
                    //#endregion
                    "sqljs",
                    //#region @backend
                    "spanner",
                    //#endregion
                ])
        }
    }
}
