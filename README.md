# Conduit Connector HubSpot

## General

The [HubSpot](https://hubspot.com/) connector is one of Conduit plugins. It provides both, a source and a destination HubSpot connector.

### Prerequisites

- [Go](https://go.dev/) v1.21
- [HubSpot](https://www.hubspot.com/) account and a [private app](https://developers.hubspot.com/docs/api/private-apps)
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint)

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests. The integration tests require the environment variable `HUBSPOT_ACCESS_TOKEN` to be set. If the environment variable is empty, the integration tests will be skipped.

## Source

The HubSpot Source Connector uses a private app access token to connect to a HubSpot account and creates records for each resource change detected in a HubSpot account.

### Snapshot capture

When the connector first starts, snapshot mode is enabled. The connector reads items of a resource that you specified. It only reads items that are created before the connector starts to run, batching them by `bufferSize`. Each new batch is processed every `pollingPeriod` duration. Once all items in that initial snapshot are read the connector switches into CDC mode.

This behavior is enabled by default, but can be turned off by adding `"snapshot": false` to the Source configuration.

### Change Data Capture

When a snapshot is captured the connector starts to listen to data changes. It can track creates, updates, and deletes that occur after the connector is started. But please note that not all resources support all operations. You can check the available resources and operations they support out [here](docs/resources.md).

### Position structure

The connector goes through two modes.

**Snapshot**. The position contains the `initialTimestamp` field that is equal to the timestamp of the first connector run. If a resource supports filtering by id, the position also contains the id of the last processed item in the `itemId` field.

Here's an example of a Snapshot position:

```json
{
  "mode": "snapshot",
  "itemId": "256",
  "initialTimestamp": "2022-10-28T14:58:27Z"
}
```

**CDC**. The position in this mode contains the same fields as in the Snapshot mode plus a `timestamp` that is equal to the `updatedAt` of the last processed item.

Here's an example of a CDC position:

```json
{
  "mode": "cdc",
  "itemId": "256",
  "initialTimestamp": "2022-10-28T14:58:27Z",
  "timestamp": "2022-10-28T15:00:50Z"
}
```

### Configuration options

| name              | description                                                                                                                                                                                                                                                                                               | required | default |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------- |
| `accessToken`     | The private app access token for accessing the HubSpot API.                                                                                                                                                                                                                                               | **true** |         |
| `resource`        | The HubSpot resource that the connector will work with.<br />You can find a list of the available resources [here](docs/resources.md).                                                                                                                                                                    | **true** |         |
| `maxRetries`      | The number of HubSpot API request retries attempts that will be tried before giving up if a request fails.                                                                                                                                                                                                | false    | `4`     |
| `pollingPeriod`   | The duration that defines a period of polling new items.                                                                                                                                                                                                                                                  | false    | `5s`    |
| `bufferSize`      | The buffer size for consumed items.<br />It will also be used as a limit when retrieving items from the HubSpot API.                                                                                                                                                                                      | false    | `100`   |
| `extraProperties` | The list of HubSpot resource properties to include in addition to the default.<br />If any of the specified properties are not present on the requested HubSpot resource, they will be ignored.<br />Only CRM resources support this.<br />The format of this field is the following: `prop1,prop2,prop3` | false    |         |
| `snapshot`        | The field determines whether or not the connector will take a snapshot of the entire collection before starting CDC mode.                                                                                                                                                                                 | false    | `true`  |

### Known limitations

- Not all resources support all CDC operations. You can check the available resources and operations they support out [here](docs/resources.md).

## Destination

The HubSpot Destination takes a `record.Record` and sends its payload to HubSpot without any transformations. The destination is designed to handle different payloads. You can check the available resources and operations they support out [here](/docs/resources.md).

### Configuration options

| name            | description                                                                                                                            | required | default |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------- |
| `accessToken`   | The private app access token for accessing the HubSpot API.                                                                            | **true** |         |
| `resource`      | The HubSpot resource that the connector will work with.<br />You can find a list of the available resources [here](docs/resources.md). | **true** |         |
| `maxRetries`    | The number of HubSpot API request retries attempts that will be tried before giving up if a request fails.                             | false    | `4`     |

### Known limitations

- To perform the update or delete operations the destination requires the `record.Key` to be set.
- If you want to use the destination to insert records from a source containing records that were previously taken by the HubSpot source, you may need to exclude some read-only fields from their payload. When trying to insert read-only fields you'll see an appropriate error message in the logs containing the names of the read-only fields.