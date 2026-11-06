# Coding Assessment

## 1. Business context

You have joined the engineering team of a large distribution and e-commerce company.

The company maintains a central Product Information Management system (PIM) and a Warehouse Management System (WMS).

Every morning, the company needs to synchronise the product catalogue from the PIM into the WMS.

The integration was built when the catalogue contained fewer than 10,000 products.

The catalogue now contains approximately **250,000 products** and is expected to reach approximately **1 million products within the next 18 months**.

The code is included in the repository folder.

It works for the current small-scale environment, but the engineering team has identified concerns around reliability, scalability and operational recovery.

You have been asked to take ownership of the integration and prepare it for production at the expected scale.

## 2. Existing integration

The current business flow is:

1. Retrieve all products from the PIM.
2. Produce a CSV representation of the catalogue.
3. Store the CSV in Amazon S3.
4. Read the catalogue data.
5. Transform it into the format expected by the WMS.
6. Send the products to the WMS in batches.

The PIM and WMS are existing systems outside your team's ownership.

The repository contains local mock versions of both systems so that you can develop and test without access to external infrastructure.

## 3. PIM API

The PIM API is paginated.

Its documented characteristics are:

- maximum 10 requests per second
- maximum page size of 500 records
- occasional HTTP 429 responses
- occasional HTTP 5xx responses
- response time can vary between approximately 100 ms and 3 seconds
- API-key authentication
- a complete catalogue export represents one logical business run

The API does not provide an endpoint that returns the entire catalogue in one response.

The mock service supplied with the repository exhibits similar pagination, throttling and transient-failure behaviour.

## 4. WMS API

The WMS accepts products in batches.

Its documented characteristics are:

- maximum batch size: 100 products
- maximum sustained request rate: 20 requests per second
- occasional HTTP 429 responses
- occasional HTTP 5xx responses
- response time can vary significantly
- individual records can be rejected for business validation reasons
- a successful request may therefore contain both accepted and rejected records

The Warehouse team has stated:

> "The same product must never be sent twice."

Treat this statement as a business requirement, but do not assume that its technical meaning is completely defined.

## 5. Operational requirements

The business currently expects:

- the daily synchronisation to normally complete within **30 minutes**
- the solution should be suitable for a scheduled, production workload and should balance reliability, scalability, operational complexity, and cost.
- temporary failures to recover automatically where practical
- operations to be able to determine whether a run is running, completed or failed
- a failure affecting a small number of products should not unnecessarily cause successfully processed products to be processed again
- the original export for each run to be retained in S3 for **90 days**
- sufficient logging and monitoring to investigate failures
- the design to support the expected growth in catalogue size

The company uses AWS and Terraform for infrastructure management.

## 6. Your assignment

Review the existing implementation and take it towards a production-ready architecture.

You may modify the existing implementation substantially where you believe that is justified.

Your solution should address, as appropriate:

### Architecture
- execution model
- orchestration
- component boundaries
- failure boundaries
- retry strategy
- concurrency
- recovery
- scalability
- operational visibility

### Data processing
- pagination
- large catalogue handling
- CSV generation
- S3 storage
- transformation
- batching
- partial processing
- recovery from interrupted runs

### External API integration
- authentication
- rate limits
- transient errors
- retries and backoff
- partial success
- validation failures
- ambiguous outcomes

### AWS
Use AWS services where appropriate.

Infrastructure should be represented using **Terraform**.

The solution should be designed as something that could reasonably be operated in production.

You do not need to deploy the solution to a live AWS account. The application should be runnable and testable locally, with appropriate mocks or local AWS emulation where required.

## 7. Requirements clarification and assumptions

As you work through the challenge, use your engineering judgement to translate the requirements into a solution. 

Ensure that you document:

1. the issue you identified
2. the assumption you made, if any
3. the effect of that assumption
4. what you would change if the assumption were not valid

There is no requirement to ask the assessment team questions during the exercise. Make reasonable assumptions and document them in such cases.

## 8. Expected deliverables

### Deliverable 1 — Architecture and justification

Create `ARCHITECTURE.md` containing:

- architecture diagram
- explanation of major components
- execution flow
- data flow
- failure/retry flow
- scaling approach
- handling of API rate limits
- handling of partial failures
- approach to duplicate/ambiguous processing
- AWS service choices
- security/IAM considerations
- observability considerations
- important trade-offs

The diagram can be Mermaid, draw.io, PlantUML, an image, or another format that can be reviewed easily.

We are interested not only in the diagram but in **why you chose the architecture**.

### Deliverable 2 — Code and infrastructure

Provide the working code and Terraform changes.

Your implementation should demonstrate the most important parts of your proposed architecture.

You do not need to implement every conceivable production feature.

Prioritise the areas where you believe engineering risk is highest.

Include tests where they provide meaningful confidence.

### Deliverable 3 — Unfinished work and next steps

Create `UNFINISHED.md`.

A complete solution is one of the evaluation criteria. If there is anything you were unable to complete, please document it clearly.

For each unfinished item, include:

- what remains unfinished
- your proposed solution if you had more time
- how you would validate that solution
- why you did not complete it
- whether the decision was deliberate prioritisation or an implementation limitation
- the risk created by leaving it unfinished
- a rough priority

**Do not hide unfinished work.**

A thoughtful explanation of an unfinished item is preferable to spending the entire 48 hours attempting to polish a low-value feature.

## 9. What we will look for

We are particularly interested in:

- architectural judgement
- production failure reasoning
- scalable system design
- AWS judgement
- Python engineering quality
- infrastructure-as-code quality
- security
- observability
- testing
- prioritisation
- clarity of communication

The assessment is not a contest to produce the largest amount of code.

## 10. Use of AI and other tools

You may use AI coding assistants, LLMs, documentation, web searches, debuggers, IDE tooling, testing tools and other development tools.

You remain responsible for the submitted solution and should be able to explain and defend the architecture, code and trade-offs during the follow-up discussion.

## 11. Submission

Return the completed repository, including:

```text
ARCHITECTURE.md
UNFINISHED.md
source code
tests
Terraform
any other files required to understand and run the solution
```

Please include instructions for running and testing your solution locally.
