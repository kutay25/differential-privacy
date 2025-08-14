- [*] Double-check whether new distinct_id uses the max_contributions correctly

- [ ] Implement BoundTotalContributions
    - [ ] Find where CB is on Python code: 
            Drop Non-Public Partition -> (pid, pk, val) **CP and PP CB** ((pid, pk), accum) -> Add Empty Public Partitions -> Accumulate -> **Private PS** -> Post Aggregation Thresholding 
    - [ ] Find where CB is on Beam's code: 
            Drop Non-Public Partition -> () **CP or PP CB** -> Add Empty Public Partitions -> Accumulate -> **Private PS** -> Post Aggregation Thresholding 
    - [ ] Determine how to implement Python version into Beam.


# Pipeline Type Formats for
## Count:
Drop Non-Public Partition -> (pid, pk, 1) **CP CB** -> Add Empty Public Partitions -> Accumulate and **PP CB** -> **Private PS** -> Post Aggregation Thresholding 