//#region @backend
import { ObjectID } from "../driver/mongodb/typings"
//#endregion

/**
 * A single property handler for FindOptionsSelect.
 */
export type FindOptionsSelectProperty<Property> = Property extends Promise<
    infer I
>
    ? FindOptionsSelectProperty<I> | boolean
    : Property extends Array<infer I>
    ? FindOptionsSelectProperty<I> | boolean
    : Property extends Function
    ? never
    //#region @backend
    : Property extends Buffer
    ? boolean
    //#endregion
    : Property extends Date
    ? boolean
    //#region @backend
    : Property extends ObjectID
    ? boolean
    //#endregion
    : Property extends object
    ? FindOptionsSelect<Property>
    : boolean

/**
 * Select find options.
 */
export type FindOptionsSelect<Entity> = {
    [P in keyof Entity]?: P extends "toString"
        ? unknown
        : FindOptionsSelectProperty<NonNullable<Entity[P]>>
}

/**
 * Property paths (column names) to be selected by "find" defined as string.
 * Old selection mechanism in TypeORM.
 *
 * @deprecated will be removed in the next version, use FindOptionsSelect type notation instead
 */
export type FindOptionsSelectByString<Entity> = (keyof Entity)[]
