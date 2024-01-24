# home-built-service
Docker image for micro service based on the Flask framework.

### Clone and build application
Build the Docker image manually by cloning the Git repo.
```
$ git clone https://github.com/twlski/home_built_cloud_service.git
$ docker build -t twolski/flask-docker-service:latest .
```

### Run the container
Create a container from the image.
```
$ docker run -v <dir_to_host_bucket_dir>:/buckets --name container -d -p 8080:8080 twolski/flask-docker-service:latest
```
Service can be reached by visiting http://localhost:8080
