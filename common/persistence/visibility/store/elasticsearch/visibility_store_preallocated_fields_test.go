package elasticsearch

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
)

const preallocatedFieldsTestIndex = "test-index"

// esClusterMetadataTypeMap returns the custom search attributes of an Elasticsearch cluster with
// legacy cluster-level search attributes and the pre-allocated fields in cluster metadata.
func esClusterMetadataTypeMap() searchattribute.NameTypeMap {
	customSearchAttributes := maps.Clone(searchattribute.TestEsNameTypeMap().Custom())
	maps.Copy(customSearchAttributes, searchattribute.TestNameTypeMap().Custom())
	return searchattribute.NewNameTypeMap(customSearchAttributes)
}

// esMappingOf returns the Elasticsearch mapping of the given search attributes, as returned by
// client.GetMapping.
func esMappingOf(searchAttributes map[string]enumspb.IndexedValueType) map[string]string {
	result := make(map[string]string, len(searchAttributes))
	for name, valueType := range searchAttributes {
		result[name] = client.FieldType(valueType)
	}
	return result
}

func newPreallocatedFieldsTestStore(
	t *testing.T,
	typeMap searchattribute.NameTypeMap,
) (*VisibilityStore, *client.MockClient, *metricstest.Capture) {
	ctrl := gomock.NewController(t)
	saProvider := searchattribute.NewMockProvider(ctrl)
	saProvider.EXPECT().GetSearchAttributes(preallocatedFieldsTestIndex, false).Return(typeMap, nil)
	esClient := client.NewMockClient(ctrl)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	return &VisibilityStore{
		esClient:                 esClient,
		index:                    preallocatedFieldsTestIndex,
		searchAttributesProvider: saProvider,
		metricsHandler:           metricsHandler,
		logger:                   log.NewNoopLogger(),
	}, esClient, capture
}

func requireUnmappedFieldsGauge(t *testing.T, capture *metricstest.Capture, expected float64) {
	t.Helper()
	recordings := capture.SnapshotMetric(metrics.ElasticsearchMissingPreallocatedFieldMappings.Name())
	require.Len(t, recordings, 1)
	require.InDelta(t, expected, recordings[0].Value, 0)
}

func TestRegisterPreallocatedFieldMappings_AddsMissingFields(t *testing.T) {
	// The index only maps the legacy cluster-level search attributes (schema older than the pool).
	s, esClient, capture := newPreallocatedFieldsTestStore(t, esClusterMetadataTypeMap())
	esClient.EXPECT().GetMapping(gomock.Any(), preallocatedFieldsTestIndex).
		Return(esMappingOf(searchattribute.TestEsNameTypeMap().Custom()), nil)
	esClient.EXPECT().PutMapping(gomock.Any(), preallocatedFieldsTestIndex, searchattribute.TestNameTypeMap().Custom()).
		Return(true, nil)
	esClient.EXPECT().WaitForYellowStatus(gomock.Any(), preallocatedFieldsTestIndex).Return("yellow", nil)

	s.registerPreallocatedFieldMappings(context.Background())
	requireUnmappedFieldsGauge(t, capture, 0)
}

func TestRegisterPreallocatedFieldMappings_AllFieldsMapped(t *testing.T) {
	typeMap := esClusterMetadataTypeMap()
	s, esClient, capture := newPreallocatedFieldsTestStore(t, typeMap)
	esClient.EXPECT().GetMapping(gomock.Any(), preallocatedFieldsTestIndex).Return(esMappingOf(typeMap.Custom()), nil)
	// No PutMapping call is expected.

	s.registerPreallocatedFieldMappings(context.Background())
	requireUnmappedFieldsGauge(t, capture, 0)
}

func TestRegisterPreallocatedFieldMappings_SkipsFieldsMappedWithAnotherType(t *testing.T) {
	typeMap := esClusterMetadataTypeMap()
	mapping := esMappingOf(searchattribute.TestEsNameTypeMap().Custom())
	// A custom search attribute named like a pre-allocated field, registered with another type.
	mapping["Text01"] = "keyword"
	expectedMissing := maps.Clone(searchattribute.TestNameTypeMap().Custom())
	delete(expectedMissing, "Text01")

	s, esClient, capture := newPreallocatedFieldsTestStore(t, typeMap)
	esClient.EXPECT().GetMapping(gomock.Any(), preallocatedFieldsTestIndex).Return(mapping, nil)
	esClient.EXPECT().PutMapping(gomock.Any(), preallocatedFieldsTestIndex, expectedMissing).Return(true, nil)
	esClient.EXPECT().WaitForYellowStatus(gomock.Any(), preallocatedFieldsTestIndex).Return("yellow", nil)

	s.registerPreallocatedFieldMappings(context.Background())
	requireUnmappedFieldsGauge(t, capture, 1)
}

func TestRegisterPreallocatedFieldMappings_PutMappingFails(t *testing.T) {
	s, esClient, capture := newPreallocatedFieldsTestStore(t, esClusterMetadataTypeMap())
	esClient.EXPECT().GetMapping(gomock.Any(), preallocatedFieldsTestIndex).
		Return(esMappingOf(searchattribute.TestEsNameTypeMap().Custom()), nil)
	esClient.EXPECT().PutMapping(gomock.Any(), preallocatedFieldsTestIndex, gomock.Any()).
		Return(false, errors.New("forbidden"))

	s.registerPreallocatedFieldMappings(context.Background())
	requireUnmappedFieldsGauge(t, capture, float64(len(searchattribute.TestNameTypeMap().Custom())))
}

func TestRegisterPreallocatedFieldMappings_SkipsWhenMappingCannotBeRead(t *testing.T) {
	for name, getMapping := range map[string]func(*client.MockClient){
		"get mapping fails": func(esClient *client.MockClient) {
			esClient.EXPECT().GetMapping(gomock.Any(), preallocatedFieldsTestIndex).Return(nil, errors.New("boom"))
		},
		"index is an alias": func(esClient *client.MockClient) {
			esClient.EXPECT().GetMapping(gomock.Any(), preallocatedFieldsTestIndex).Return(map[string]string{}, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, esClient, capture := newPreallocatedFieldsTestStore(t, esClusterMetadataTypeMap())
			getMapping(esClient)
			// No PutMapping call is expected.

			s.registerPreallocatedFieldMappings(context.Background())
			require.Empty(t, capture.SnapshotMetric(metrics.ElasticsearchMissingPreallocatedFieldMappings.Name()))
		})
	}
}

func TestRegisterPreallocatedFieldMappings_NoPreallocatedFields(t *testing.T) {
	s, _, capture := newPreallocatedFieldsTestStore(t, searchattribute.TestEsNameTypeMap())
	// No GetMapping or PutMapping call is expected.

	s.registerPreallocatedFieldMappings(context.Background())
	require.Empty(t, capture.SnapshotMetric(metrics.ElasticsearchMissingPreallocatedFieldMappings.Name()))
}
