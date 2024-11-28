version = "1.0.0"

dependencies {
    implementation("org.apache.spark:spark-core_2.13:3.5.3")
    compileOnly("org.apache.spark:spark-sql_2.13:3.5.3")

    testImplementation("org.apache.spark:spark-sql_2.13:3.5.3")
}