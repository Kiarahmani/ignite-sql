alias init='export MAVEN_OPTS="-Xmx850m" && mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="init"'
alias follow='git pull && mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="false 1"'
alias coordinate='gut pull && mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="true 1"'
alias run='mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="none"'
