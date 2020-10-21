# Contributing to .NET for Apache Spark!

If you are here, it means you are interested in helping us out. A hearty welcome and thank you! There are many ways you can contribute to the .NET for Apache Spark project:

* Offer PR's to fix bugs or implement new features
* Review currently [open PRs](https://github.com/dotnet/spark/pulls)
* Give us feedback and bug reports regarding the software or the documentation
* Improve our examples, tutorials, and documentation

Please start by browsing the [issues](https://github.com/dotnet/spark/issues) and leave a comment to engage us if any of them interests you. And don't forget to take a look at the project [roadmap](https://github.com/dotnet/spark/blob/master/ROADMAP.md).

Here are a few things to consider:

* Before starting working on a major feature or bug fix, please open a GitHub issue describing the work you are proposing. We will make sure no one else is already working on it and the work aligns with the project [roadmap](https://github.com/dotnet/spark/blob/master/ROADMAP.md).
* A "major" feature or bug fix is defined as any change that is > 100 lines of code (not including tests) or changes user-facing behavior (e.g., breaking API changes). Please read [Proposing Major Changes to .NET for Apache Spark](#proposing-major-changes-to-net-for-apache-spark) before you begin any major work.
* Once you are ready, you can create a PR and the committers will help reviewing your PR.

**Coding Style**: Please review our [coding guidelines](https://github.com/rapoth/spark-2/blob/master/docs/contributing.md). 

## Getting started

Please make sure to take a look at the project [roadmap](ROADMAP.md).

### Pull requests

If you are new to GitHub [here](https://help.github.com/categories/collaborating-with-issues-and-pull-requests/) is a detailed help source on getting involved with development on GitHub.

As a first time contributor, you will be invited to sign the Contributor License Agreement (CLA). Please follow the instructions of the dotnet foundation bot reviewer on your PR to sign the agreement indicating that you have appropriate rights to your contribution.

Your pull request needs to reference a filed issue. Please fill in the template that is populated for the pull request. Only pull requests addressing small typos can have no issues associated with them.

A .NET for Apache Spark team member will be assigned to your pull request once the continuous integration checks have passed successfully.

All commits in a pull request will be squashed to a single commit with the original creator as author.

### Contributing

See [Contributing](docs/contributing.md) for information about coding styles, source structure, making pull requests, and more.

### Developers

See the [Developer Guide](docs/developer-guide.md) for details about developing in this repo.

## Proposing major changes to .NET for Apache Spark

The development process in .NET for Apache Spark is design-driven. If you intend of making any significant changes, please consider discussing with the .NET for Apache Spark community first (and sometimes formally documented), before you open a PR.

The rest of this document describes the process for proposing, documenting and implementing changes to the .NET for Apache Spark project.

To learn about the motivation behind .NET for Apache Spark, see the following talk:
  - [Introducing .NET Bindings for Apache Spark](https://databricks.com/session/introducing-net-bindings-for-apache-spark) from Spark+AI Summit 2019.
  - [.NET for Apache Spark](https://databricks.com/session_eu19/net-for-apache-spark) from Spark+AI Europe Summit 2019.

### The Proposal Process

The process outlined below is for reviewing a proposal and reaching a decision about whether to accept/decline a proposal.	

  1. The proposal author [creates a brief issue](https://github.com/dotnet/spark/issues/new?assignees=&labels=untriaged%2C+proposal&template=design-template.md&title=%5BPROPOSAL%5D%3A+) describing the proposal.
  2. A discussion on the issue will aim to triage the proposal into one of three outcomes:
     - Accept proposal
     - Decline proposal
     - Ask for a detailed doc
     If the proposal is accepted/declined, the process is done. Otherwise, the discussion is expected to identify concerns that should be addressed in a more detailed design.
  3. The proposal author follows up with a detailed description to work out details of the proposed design and address the concerns raised in the initial discussion.
  4. Once comments and revisions on the design are complete, there is a final discussion on the issue to reach one of two outcomes:
     - Accept proposal
     - Decline proposal

After the proposal is accepted or declined (e.g., after Step 2 or Step 4), implementation work proceeds in the same way as any other contribution. 

> **Tip:** If you are an experienced committer and are certain that a design description will be required for a particular proposal, you can skip Step 2.

### Writing a Design Document

As noted [above](#the-proposal-process), some (but not all) proposals need to be elaborated in a design description.	

  - The design description should follow the template outlined below
  ```
      Proposal:
    
      Rationale:
      
      Compatibility:
      
      Design:
      
      Implementation:
      
      Impact on Performance (if applicable):
      
      Open issues (if applicable):
  ``` 

  - Once you have the design description ready and have addressed any specific concerns raised during the initial discussion, please reply back to the original issue. 
  - Address any additional feedback/questions and update your design description as needed. 

### Proposal Review

A group of .NET for Apache Spark team members will review your proposal and CC the relevant developers, raising important questions, pinging lapsed discussions, and generally trying to guide the discussion toward agreement about the outcome. The discussion itself is expected to happen on the issue, so that anyone can take part.	

### Consensus and Disagreement

The goal of the proposal process is to reach general consensus about the outcome in a timely manner.	

If general consensus cannot be reached, the proposal review group decides the next step by reviewing and discussing the issue and reaching a consensus among themselves. 

## Becoming a .NET for Apache Spark Committer

The .NET for Apache Spark team will add new committers from the active contributors, based on their contributions to the .NET for Apache Spark project. The qualifications for new committers are derived from [Apache Spark Contributor Guide](https://spark.apache.org/contributing.html):

  - **Sustained contributions to .NET for Apache Spark**: Committers should have a history of major contributions to .NET for Apache Spark. An ideal committer will have contributed broadly throughout the project, and have contributed at least one major component where they have taken an “ownership” role. An ownership role means that existing contributors feel that they should run patches for this component by this person.
  - **Quality of contributions**: Committers more than any other community member should submit simple, well-tested, and well-designed patches. In addition, they should show sufficient expertise to be able to review patches, including making sure they fit within .NET for Apache Spark’s engineering practices (testability, documentation, API stability, code style, etc). The committership is collectively responsible for the software quality and maintainability of .NET for Apache Spark. 
  - **Community involvement**: Committers should have a constructive and friendly attitude in all community interactions. They should also be active on the dev and user list and help mentor newer contributors and users. In design discussions, committers should maintain a professional and diplomatic approach, even in the face of disagreement.
