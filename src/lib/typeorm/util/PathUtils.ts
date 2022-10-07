import { hash } from "./StringUtils"

const WINDOWS_PATH_REGEXP = /^([a-zA-Z]:.*)$/
const UNC_WINDOWS_PATH_REGEXP = /^\\\\(\.\\)?(.*)$/

export function toPortablePath(filepath: string): string {
  //#region @backendFunc
    if (process.platform !== `win32`) return filepath

    if (filepath.match(WINDOWS_PATH_REGEXP))
        filepath = filepath.replace(WINDOWS_PATH_REGEXP, `/$1`)
    else if (filepath.match(UNC_WINDOWS_PATH_REGEXP))
        filepath = filepath.replace(
            UNC_WINDOWS_PATH_REGEXP,
            (match, p1, p2) => `/unc/${p1 ? `.dot/` : ``}${p2}`,
        )

    return filepath.replace(/\\/g, `/`)
    //#endregion
}

/**
 * Create deterministic valid database name (class, database) of fixed length from any filepath. Equivalent paths for windows/posix systems should
 * be equivalent to enable portability
 */
export function filepathToName(filepath: string): string {
  //#region @backendFunc
    const uniq = toPortablePath(filepath).toLowerCase()
    return hash(uniq, { length: 63 })
    //#endregion
}

/**
 * Cross platform isAbsolute
 */
export function isAbsolute(filepath: string): boolean {
  //#region @backendFunc
    return !!filepath.match(/^(?:[a-z]:|[\\]|[\/])/i)
    //#endregion
}
