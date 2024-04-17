# Mailersend

## Sync overview

This source can sync data from the [Mailersend](https://developers.mailersend.com/#mailersend-api). At present this connector only supports full refresh syncs meaning that each time you use the connector it will sync all available records from scratch.

## This Source Supports the Following Streams

- [activity](https://developers.mailersend.com/api/v1/activity.html#get-a-list-of-activities)

### Features

| Feature           | Supported?\(Yes/No\) | Notes |
| :---------------- | :------------------- | :---- |
| Full Refresh Sync | Yes                  |       |
| Incremental Sync  | No                   |       |

### Performance considerations

MailerSend has a default [rate limit](https://developers.mailersend.com/general.html#api-response) of 60 requests per minute on general API endpoints.

## Getting started

## Changelog

| Version | Date       | Pull Request                                             | Subject                                  |
| :------ | :--------- | :------------------------------------------------------- | :--------------------------------------- |
| 0.1.2 | 2024-04-15 | [37192](https://github.com/airbytehq/airbyte/pull/37192) | Base image migration: remove Dockerfile and use the python-connector-base image |
| 0.1.1 | 2024-04-12 | [37192](https://github.com/airbytehq/airbyte/pull/37192) | schema descriptions |
| 0.1.0   | 2022-11-13 | [18669](https://github.com/airbytehq/airbyte/pull/18669) | 🎉 New Source: Mailersend [low-code CDK] |
