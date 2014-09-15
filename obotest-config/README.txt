This module should be used to package configuration files that need to be 
available on the class path to code packaged elsewhere in this application.

All configuration files, e.g., logback.xml, should be placed in:

	src/main/resources

This module is specified as a dependency of the EAR module. As a result, the
JAR file that is generated from this module is packaged in the EAR's /lib
directory. All JARs in the EAR's /lib directory are available to all of the
EAR's modules (EJB, WAR, ...). Hence, any configuration file that is expected to
be loaded from the class path will be found.
