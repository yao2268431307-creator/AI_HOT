# Release builds must provide an immutable repository@sha256 base image.
ARG NODE_BASE_IMAGE
FROM ${NODE_BASE_IMAGE} AS build
WORKDIR /app
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM ${NODE_BASE_IMAGE}
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /app /app
USER node
CMD ["npm", "run", "start", "--", "-H", "0.0.0.0", "-p", "3000"]
