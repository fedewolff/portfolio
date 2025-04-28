# Influencer Responsiveness Query

Note: Table and project names have been renamed for confidentiality purposes.

## Overview

This SQL query analyzes Influencer Responsiveness to brand communications. It identifies how quickly influencers react to initial outreach events, classifying their response (or lack thereof).

The query traces event sequences and computes:

- Time to first influencer response (in seconds)

- Whether an influencer responded

- Role and type of first and second events

- Influencer location and country categorization

- Only first interactions per campaign are considered to avoid duplicates.
