Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Running Locally
```docker build -t dbt_transformation_image .```
```
docker run --rm \
  -e CLIENT_NAME=demo \
  -e GCP_PROJECT=abcdataz \
  -e PORT=8080 \
  -p 8080:8080 \
  dbt_transformation_image
```