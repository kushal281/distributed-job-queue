from locust import HttpUser, between, task


class Submitter(HttpUser):
    wait_time = between(0, 0.1)

    @task(10)
    def submit(self):
        self.client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0.2}}, name="POST /jobs")

    @task(1)
    def stats(self):
        self.client.get("/stats", name="GET /stats")