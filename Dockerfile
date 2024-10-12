# Use the official Node.js v20.12.1 image as the base image
FROM node:20.12.1

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json to leverage Docker cache
COPY package*.json ./

# Install Node.js dependencies
RUN npm install

# Copy the rest of the application code
COPY . .

# Ensure the entrypoint script has execute permissions
RUN chmod +x entrypoint.sh

# Expose any necessary ports (if your scripts start a server)
# EXPOSE 3000  # Uncomment and modify if needed

# Specify the entrypoint script
ENTRYPOINT ["./entrypoint.sh"]
