- Add a `TypeOf` operation (not a fix for #1802 yet, because it doesn’t
  implement it for any connector)
- adds a new PrimaryType type that reflects the simpler notion of type
  used with “patterns” as we go forward (and also the notion of types
  used by `TypeOf`)
- use the sbt-travisci plugin
- convert `Bson.Date` to use a `Long` instead of `Instant` (since
  `Instant` encodes more values than `Bson.Date`)
