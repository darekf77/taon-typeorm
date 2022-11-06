
//#region @backend
import * as path from "path"
import * as fs from "fs"
import * as dotenv from "dotenv"
import chalkBackend from "chalk"
import { highlight as highlightBackend, Theme as ThemeBackend } from "cli-highlight"

export { ReadStream } from "fs"
export { EventEmitter } from "events"
export { Readable, Writable } from "stream"
//#endregion

interface Theme
//#region @backend
 extends ThemeBackend
 //#endregion
 {

}


let highlight
//#region @backend
= highlightBackend
//#endregion
//#region @websqlOnly
highlight = (args) => args;
//#endregion

let chalk
  //#region @backend
  = chalkBackend;
//#endregion
//#region @websqlOnly
chalk = { // chalk websql mock
  // @ts-ignore
  blueBright(args) {
    return args;
  },
  // @ts-ignore
  white(args) {
    return args;
  },
  // @ts-ignore
  gray(args) {
    return args;
  },
  // @ts-ignore
  magenta(args) {
    return args;
  },
  // @ts-ignore
  magentaBright(args) {
    return args;
  },
  // @ts-ignore
  yellow(args) {
    return args;
  },
  // @ts-ignore
  red(args) {
    return args;
  },
  // @ts-ignore
  bgRed(args) {
    return args;
  },
  // @ts-ignore
  underline(args) {
    return args;
  },
  // @ts-ignore
  black(args) {
    return args;
  },
};

Object.keys(chalk).forEach(key => {
  Object.keys(chalk).forEach(key2 => {
    chalk[key][key2] = chalk[key];
  });
});


//#endregion


/**
 * Platform-specific tools.
 */
export class PlatformTools {
  /**
   * Type of the currently running platform.
   */
  static get type(): 'node' | 'browser' {
    //#region @backend
    // @ts-ignore
    return 'node';
    //#endregion
    // @ts-ignore
    return 'browser';
  }

  /**
   * Gets global variable where global stuff can be stored.
   */
  static getGlobalVariable(): any {
    //#region @websqlOnly
    // @ts-ignore
    window.global = window;
    // @ts-ignore
    return window.global;
    //#endregion
    //#region @backendFunc
    return global
    //#endregion
  }

  /**
   * Loads ("require"-s) given file or package.
   * This operation only supports on node platform
   */
  static load(name: string): any {
    // if name is not absolute or relative, then try to load package from the node_modules of the directory we are currently in
    // this is useful when we are using typeorm package globally installed and it accesses drivers
    // that are not installed globally

    try {
      // switch case to explicit require statements for webpack compatibility.
      switch (name) {
        /**
         * spanner
         */
        case "spanner":
          //#region @backendFunc
          return require("@google-cloud/spanner")
        //#endregion

        /**
         * mongodb
         */
        case "mongodb":
          //#region @backendFunc
          return require("mongodb")
        //#endregion

        /**
         * hana
         */
        case "@sap/hana-client":
          //#region @backendFunc
          return require("@sap/hana-client")
        //#endregion

        case "hdb-pool":
          //#region @backendFunc
          return require("hdb-pool")
        //#endregion

        /**
         * mysql
         */
        case "mysql":
          //#region @backendFunc
          return require("mysql")
        //#endregion

        case "mysql2":
          //#region @backendFunc
          return require("mysql2")
        //#endregion

        /**
         * oracle
         */
        case "oracledb":
          //#region @backendFunc
          return require("oracledb")
        //#endregion

        /**
         * postgres
         */
        case "pg":
          //#region @backendFunc
          return require("pg")
        //#endregion

        case "pg-native":
          //#region @backendFunc
          return require("pg-native")
        //#endregion

        case "pg-query-stream":
          //#region @backendFunc
          return require("pg-query-stream")
        //#endregion

        case "typeorm-aurora-data-api-driver":
          //#region @backendFunc
          return require("typeorm-aurora-data-api-driver")
        //#endregion

        /**
         * redis
         */
        case "redis":
          //#region @backendFunc
          return require("redis")
        //#endregion

        case "ioredis":
          //#region @backendFunc
          return require("ioredis")
        //#endregion

        /**
         * better-sqlite3
         */
        case "better-sqlite3":
          //#region @backendFunc
          return require("better-sqlite3")
        //#endregion

        /**
         * sqlite
         */
        case "sqlite3":
          //#region @backendFunc
          return require("sqlite3")
        //#endregion

        /**
         * sql.js
         */
        case "sql.js":
          // @ts-ignore
          return require("sql.js")
        /**
         * sqlserver
         */
        case "mssql":
          //#region @backendFunc
          return require("mssql")
        //#endregion

        /**
         * react-native-sqlite
         */
        case "react-native-sqlite-storage":
          //#region @backendFunc
          return require("react-native-sqlite-storage")
        //#endregion
      }
    } catch (err) {
      //#region @backendFunc
      return require(path.resolve(
        process.cwd() + "/node_modules/" + name,
      ))
      //#endregion
    }

    // If nothing above matched and we get here, the package was not listed within PlatformTools
    // and is an Invalid Package.  To make it explicit that this is NOT the intended use case for
    // PlatformTools.load - it's not just a way to replace `require` all willy-nilly - let's throw
    // an error.
    throw new TypeError(`Invalid Package for PlatformTools.load: ${name}`)
  }

  /**
   * Normalizes given path. Does "path.normalize".
   */
  static pathNormalize(pathStr: string): string {
    //#region @backendFunc
    return path.normalize(pathStr)
    //#endregion
  }

  /**
   * Gets file extension. Does "path.extname".
   */
  static pathExtname(pathStr: string): string {
    //#region @backendFunc
    return path.extname(pathStr)
    //#endregion
  }

  /**
   * Resolved given path. Does "path.resolve".
   */
  static pathResolve(pathStr: string): string {
    //#region @backendFunc
    return path.resolve(pathStr)
    //#endregion
  }

  /**
   * Synchronously checks if file exist. Does "fs.existsSync".
   */
  static fileExist(pathStr: string): boolean {
    //#region @backendFunc
    return fs.existsSync(pathStr)
    //#endregion
  }

  // @ts-ignore
  static readFileSync(filename: string): Buffer {
    //#region @backendFunc
    return fs.readFileSync(filename)
    //#endregion
  }

  static appendFileSync(filename: string, data: any): void {
    //#region @backendFunc
    fs.appendFileSync(filename, data)
    //#endregion
  }

  static async writeFile(path: string, data: any): Promise<void> {
    return new Promise<void>((ok, fail) => {
      //#region @backend
      fs.writeFile(path, data, (err) => {
        if (err) fail(err)
        ok()
      })
      //#endregion
    })
  }

  /**
   * Loads a dotenv file into the environment variables.
   *
   * @param path The file to load as a dotenv configuration
   */
  static dotenv(pathStr: string): void {
    //#region @backend
    dotenv.config({ path: pathStr })
    //#endregion
  }

  /**
   * Gets environment variable.
   */
  static getEnvVariable(name: string): any {
    //#region @backendFunc
    return process.env[name]
    //#endregion
  }

  /**
   * Highlights sql string to be print in the console.
   */
  static highlightSql(sql: string) {
    //#region @websqlFunc
    const theme: Theme = {
      keyword: chalk.blueBright,
      literal: chalk.blueBright,
      string: chalk.white,
      type: chalk.magentaBright,
      built_in: chalk.magentaBright,
      comment: chalk.gray,
    }
    return highlight(sql, { theme: theme, language: "sql" })
    //#endregion
  }

  /**
   * Highlights json string to be print in the console.
   */
  static highlightJson(json: string) {
    //#region @websqlFunc
    return highlight(json, { language: "json" })
    //#endregion
  }

  /**
   * Logging functions needed by AdvancedConsoleLogger
   */
  static logInfo(prefix: string, info: any) {
    //#region @websql
    console.log(chalk.gray.underline(prefix), info)
    //#endregion
  }

  static logError(prefix: string, error: any) {
    //#region @websql
    console.log(chalk.underline.red(prefix), error)
    //#endregion
  }

  static logWarn(prefix: string, warning: any) {
    //#region @websql
    console.log(chalk.underline.yellow(prefix), warning)
    //#endregion
  }

  static log(message: string) {
    //#region @websql
    console.log(chalk.underline(message))
    //#endregion
  }

  static warn(message: string) {
    //#region @websql
    return chalk.yellow(message)
    //#endregion
  }

  static logCmdErr(prefix: string, err?: any) {
    //#region @websql
    console.log(chalk.black.bgRed(prefix))
    if (err) console.error(err)
    //#endregion
  }
}
