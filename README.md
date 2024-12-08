By [Dmytro Nasyrov, Founder, CTO at Pharos Production Inc.](https://www.linkedin.com/in/dmytronasyrov/)
And [Pharos Production Inc. - Web3, blockchain, fintech, defi software development services](https://pharosproduction.com)

-Dsun.io.serialization.extendedDebugInfo=true --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED

Flink recognizes a data type as a POJO type (and allows “by-name” field referencing) if the following conditions are fulfilled:
The class is public and standalone (no non-static inner class)
The class has a public no-argument constructor
All non-static, non-transient fields in the class (and all superclasses) are either public (and non-final) or have a public getter- and a setter- method that follows the Java beans naming conventions for getters and setters.
