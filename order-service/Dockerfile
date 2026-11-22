# ---- Build: compila o jar com o Maven Wrapper ----
FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace

COPY mvnw pom.xml ./
COPY .mvn .mvn
# Garante quebras de linha LF e permissão de execução (arquivos vindos do Windows)
RUN sed -i 's/\r$//' mvnw && chmod +x mvnw

COPY src src

# O cache do ~/.m2 é mantido entre builds, então as dependências não são baixadas de novo.
# Os testes ficam de fora: eles usam Testcontainers e rodam no CI, não dentro do build da imagem.
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -ntp package -DskipTests

# ---- Runtime: só o JRE e o jar, rodando sem root ----
FROM eclipse-temurin:25-jre
WORKDIR /app

RUN groupadd --system app && useradd --system --gid app app
COPY --from=build /workspace/target/*.jar app.jar
USER app

EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
