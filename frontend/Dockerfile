# =====================================================================================================
# The nginx image with the React frontend inside. docker-compose.yml builds it for the
# "nginx" service (profile "cluster"):
#
#   docker compose --profile cluster up -d --build     ...then open http://localhost:8080
#
# Two stages: Node builds the frontend (npm run build -> dist/), then only dist/ is copied into nginx. The final
# image has no Node, no node_modules and no source code. nginx's config is not baked in: docker-compose.yml mounts
# nginx/templates/, as before.
# =====================================================================================================

# ---- Stage 1: build the static files ----
FROM node:22.18.0-alpine AS build
WORKDIR /src
# Layer caching: install the packages first. Docker reuses this layer until package-lock.json changes.
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# ---- Stage 2: nginx serves them ----
FROM nginx:1.27-alpine
COPY --from=build /src/dist /usr/share/nginx/html
