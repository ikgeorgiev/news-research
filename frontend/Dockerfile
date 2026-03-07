FROM node:22-alpine

WORKDIR /app/frontend

COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

COPY frontend ./
ARG NEXT_PUBLIC_API_BASE
RUN test -n "$NEXT_PUBLIC_API_BASE" || (echo "NEXT_PUBLIC_API_BASE build arg is required for the frontend image" >&2 && exit 1)
ENV NEXT_PUBLIC_API_BASE=$NEXT_PUBLIC_API_BASE
RUN npm run build

ENV NODE_ENV=production

EXPOSE 3000

CMD ["npm", "run", "start", "--", "--hostname", "0.0.0.0", "--port", "3000"]
