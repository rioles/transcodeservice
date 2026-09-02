
# =========================================================
# STAGE 1: Build JAR
# =========================================================
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder

WORKDIR /app

# Copier le POM en premier pour profiter du cache Docker
COPY pom.xml .

# Pré-télécharger les dépendances Maven
RUN mvn dependency:go-offline

# Copier le code source
COPY src ./src

# Compiler et générer le JAR
# Les tests sont déjà exécutés dans la CI GitHub Actions
RUN mvn package -DskipTests


# =========================================================
# STAGE 2: Runtime Image
# =========================================================
FROM eclipse-temurin:21-jre-alpine

# Installer FFmpeg et les dépendances système nécessaires
RUN apk add --no-cache \
    ffmpeg \
    bash \
    tzdata

WORKDIR /app

# Workspace temporaire dédié au transcodage
# Peut ensuite être monté avec un emptyDir/tmpfs dans Kubernetes
RUN mkdir -p /tmp/transcoder-workspace && \
    chmod 777 /tmp/transcoder-workspace

# Copier uniquement le JAR depuis le stage de build
COPY --from=builder /app/target/*.jar app.jar

# Variables d'environnement
ENV TRANSCODER_WORKSPACE=/tmp/transcoder-workspace
ENV JAVA_OPTS="-Xms512m -Xmx2048m"

EXPOSE 8080

# Démarrage du service
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Djava.io.tmpdir=/tmp/transcoder-workspace -jar app.jar"]


