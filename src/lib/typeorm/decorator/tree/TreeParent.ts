import { getMetadataArgsStorage } from "../../globals"
import { RelationMetadataArgs } from "../../metadata-args/RelationMetadataArgs"
import { OnDeleteType } from "../../metadata/types/OnDeleteType"
import { RelationOptions } from "../options/RelationOptions"

/**
 * Marks a entity property as a parent of the tree.
 * "Tree parent" indicates who owns (is a parent) of this entity in tree structure.
 */
export function TreeParent(options?: {
    onDelete?: OnDeleteType
}): PropertyDecorator {
    return function (object: Object, propertyName: string) {
        if (!options) options = {} as RelationOptions

        // now try to determine it its lazy relation

        let reflectedType = undefined
        //#region @backend
        reflectedType =  Reflect && (Reflect as any).getMetadata
                ? Reflect.getMetadata("design:type", object, propertyName)
                : undefined
                //#endregion
        let isLazy = false
        //#region @backend
        isLazy = (reflectedType &&
                typeof reflectedType.name === "string" &&
                reflectedType.name.toLowerCase() === "promise") ||
            false
            //#endregion

        getMetadataArgsStorage().relations.push({
            isTreeParent: true,
            target: object.constructor,
            propertyName: propertyName,
            isLazy: isLazy,
            relationType: "many-to-one",
            type: () => object.constructor,
            options: options,
        } as RelationMetadataArgs)
    }
}
