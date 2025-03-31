Currently this repo is for managing a Garmin Map workflow in Dbt. This is my first time using it so serves as a tutorial as well.

## Workflow

Data needs to be ingested into the garmin.activities source table in Google Big Query. Then

### Staging

Some column renaming and removing nulls

### Intermediate

Heavier reshaping of the data and bringing in some key metrics that will be used to calculate speed and distance

### Marts

Three fact tables are created, these are used to plot data in my [kepler map](https://hallam-flda.github.io/garmin_map/)

- fct_activity_log
- fct_activity_log_cond
- fct_travel_log



## Resources (from template bu still useful:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
