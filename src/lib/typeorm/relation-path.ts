type Join<K, P> = K extends string
  ? P extends string
    ? `${K}.${P}`
    : never
  : never;

export type RelationPath<T, Depth extends number = 3> = Depth extends 0
  ? never
  : {
      [K in keyof T]: T[K] extends object
        ? T[K] extends Array<infer U>
          ? K | Join<K, RelationPath<U, Prev[Depth]>>
          : K | Join<K, RelationPath<T[K], Prev[Depth]>>
        : K;
    }[keyof T];

type Prev = [never, 0, 1, 2, 3, 4, 5, 6];
