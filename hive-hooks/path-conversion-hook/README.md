# Path Conversion Hook
This jar implements a Hive client-side metastore filter in class `ApiaryMetastoreFilter`.

This path conversion hook allows us to arbitrarily alter partition and table paths via matching regex expressions
for a given table without altering the underlying data. This is useful for cases such as using Alluxio as a caching 
layer, where we can alter these location pointers on a client basis to improve read speeds. 

Hive ignores any metastore filter hooks if the default authorizer scheme
is in use, or if `hive.security.authorization.manager` is set to instance of `HiveAuthorizerFactory` 
(which it is by default). In order to work around this, the class `ApiaryNullAuthorizationProvider` implements 
the Hive Authorization Provider interface `HiveAuthorizationProvider`. This 
class just authorizes all access, so this would not be  appropriate in an environment 
where client-side authorization is in use. If this filter hook is
required, Ranger authentication should be configured at the metastore level.

## Enabled Hooks
| Hook Type       | Enabled? |
|-----------------|----------|
| Table(s)        | true     |
| Partition(s)    | true     |
| Database(s)     | false    |
| Index(es)       | false    |
| Table Names     | false    |
| Partition Specs | false    |
| Index Names     | false    |

## Installation
1. Copy the JAR `target/apiary-metastore-filter-<version>-all.jar` to
   the classpath of Hive. On AWS EMR, this is `/usr/lib/hive/lib`.
2. Add the following section to the `hive-site.xml`. For EMR, this will
   take the form of adding `"Classification": "hive-site"` properties in
   the EMR cluster definition.
   
   ```
      <property>
        <name>hive.security.authorization.manager</name>
        <value>com.expediagroup.apiary.extensions.hooks.pathconversion.providers.ApiaryNullAuthorizationProvider</value>
      </property>
    
      <property>
        <name>hive.metastore.filter.hook</name>
        <value>com.expediagroup.apiary.extensions.hooks.pathconversion.filters.ApiaryMetastoreFilter</value>
      </property>
   ```
   
3. By default, we leave the hook disabled unless otherwise specified. In order to use this hook you must explicitly 
   set a `hive-site.xml` property named `apiary.path.replacement.enabled` to a truthy value ("true", "True", "yes", etc).
   ```
    <property>
        <name>apiary.path.replacement.enabled</name>
        <value>true</value>
    </property>
   ```
4. In order to properly set a regex and replacer pattern, you must add the following properties for regex, value and (optionally)
   capture groups. This should be set with a suffix qualifier to tie all of them together, like so:
    ```
    <property>
        <name>apiary.path.replacement.regex.<MY_FOO_REPLACER></name>
        <value>(foo).(baz)</value>
    </property>
    <property>
        <name>apiary.path.replacement.value.<MY_FOO_REPLACER></name>
        <value>bar</value>
    </property>
   <property>
       <name>apiary.path.replacement.capturegroups.<MY_FOO_REPLACER></name>
       <value>1,2</value>
   </property>
    ```
   **NOTE**: Each defined key-value pair of these regex and value matchers must have a matching pair or else the hook will ignore it.
   
## Configurations

| Property                                | Description                                                                            | Default |
|-----------------------------------------|----------------------------------------------------------------------------------------|---------|
| apiary.path.replacement.enabled         | Boolean to determine if we should enable path replacement aspect of this Hive hook.    | false   |
| apiary.path.replacement.regex.*         | Defined regex patterns to check for replacement. Requires matching value.              | []      |
| apiary.path.replacement.value.*         | Defined value patterns to check for replacement. Requires matching regex.              | []      |
| apiary.path.replacement.capturegroups.* | (Optional) Comma delimited list of capture group indexes to use for regex replacement. | [1]     |
