FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package.json package-lock.json ./
RUN npm install

# Build the app is handled by 'npm run dev' locally for now
# For dev environment, we just run dev server
# For production, we would build and serve with Nginx

COPY . .

# Expose port
EXPOSE 3000

# Start Vite server
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "3000"]
