import React, { useEffect, useState, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  Breadcrumb,
  BreadcrumbItem,
  Button,
  Card,
  CardBody,
  CardTitle,
  DescriptionList,
  DescriptionListGroup,
  DescriptionListTerm,
  DescriptionListDescription,
  FormGroup,
  Label,
  TextArea,
  TextInput,
  Title,
  Spinner,
} from "@patternfly/react-core";
import { Table, Thead, Tr, Th, Tbody, Td } from "@patternfly/react-table";
import {
  Dataset,
  PipelineRef,
  SchemaField,
  getDataset,
  getDatasetPipelines,
  updateDataset,
  deleteDataset,
} from "../api";

export default function DatasetDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [ds, setDs] = useState<Dataset | null>(null);
  const [pipelines, setPipelines] = useState<PipelineRef[]>([]);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState(false);
  const [editName, setEditName] = useState("");
  const [desc, setDesc] = useState("");
  const [tagsStr, setTagsStr] = useState("");
  const [schemaStr, setSchemaStr] = useState("");

  const load = useCallback(async () => {
    if (!id) return;
    setLoading(true);
    try {
      const [d, pResp] = await Promise.all([
        getDataset(id),
        getDatasetPipelines(id),
      ]);
      setDs(d);
      setPipelines(pResp.pipelines);
      setEditName(d.name);
      setDesc(d.description || "");
      setTagsStr(d.tags?.join(", ") || "");
      setSchemaStr(
        d.schema_fields ? JSON.stringify(d.schema_fields, null, 2) : ""
      );
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    load();
  }, [load]);

  if (loading) return <Spinner />;
  if (!ds) return <p>Dataset not found</p>;

  const handleSave = async () => {
    await updateDataset(ds.id, {
      name: editName,
      description: desc || null,
      tags: tagsStr
        ? tagsStr
            .split(",")
            .map((t) => t.trim())
            .filter(Boolean)
        : null,
      schema_fields: schemaStr ? JSON.parse(schemaStr) : null,
    });
    setEditing(false);
    load();
  };

  const handleDelete = async () => {
    await deleteDataset(ds.id);
    navigate("/datasets");
  };

  return (
    <>
      <Breadcrumb style={{ marginBottom: "1rem" }}>
        <BreadcrumbItem onClick={() => navigate("/datasets")} component="button">
          Datasets
        </BreadcrumbItem>
        <BreadcrumbItem isActive>{ds.name}</BreadcrumbItem>
      </Breadcrumb>

      <Title headingLevel="h1" style={{ marginBottom: "1rem" }}>
        {ds.name}
      </Title>

      <Card style={{ marginBottom: "1rem" }}>
        <CardTitle>Identity</CardTitle>
        <CardBody>
          <DescriptionList isHorizontal>
            <DescriptionListGroup>
              <DescriptionListTerm>ID</DescriptionListTerm>
              <DescriptionListDescription>
                <code>{ds.id}</code>
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Source</DescriptionListTerm>
              <DescriptionListDescription>
                <code>{ds.source}</code>
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Created</DescriptionListTerm>
              <DescriptionListDescription>
                {new Date(ds.created_at).toLocaleString()}
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Updated</DescriptionListTerm>
              <DescriptionListDescription>
                {new Date(ds.updated_at).toLocaleString()}
              </DescriptionListDescription>
            </DescriptionListGroup>
          </DescriptionList>
        </CardBody>
      </Card>

      <Card style={{ marginBottom: "1rem" }}>
        <CardTitle>OpenLineage Identity (derived from source)</CardTitle>
        <CardBody>
          <DescriptionList isHorizontal>
            <DescriptionListGroup>
              <DescriptionListTerm>Namespace</DescriptionListTerm>
              <DescriptionListDescription>
                <code>{ds.ol_namespace}</code>
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Name</DescriptionListTerm>
              <DescriptionListDescription>
                <code>{ds.ol_name}</code>
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Node ID</DescriptionListTerm>
              <DescriptionListDescription>
                <code>dataset:{ds.ol_namespace}:{ds.ol_name}</code>
              </DescriptionListDescription>
            </DescriptionListGroup>
          </DescriptionList>
        </CardBody>
      </Card>

      <Card style={{ marginBottom: "1rem" }}>
        <CardTitle>
          Metadata
          {!editing && (
            <Button
              variant="secondary"
              size="sm"
              style={{ marginLeft: "1rem" }}
              onClick={() => setEditing(true)}
            >
              Edit
            </Button>
          )}
        </CardTitle>
        <CardBody>
          {editing ? (
            <>
              <FormGroup label="Name" fieldId="edit-name">
                <TextInput
                  id="edit-name"
                  value={editName}
                  onChange={(_e, v) => setEditName(v)}
                />
              </FormGroup>
              <FormGroup
                label="Description"
                fieldId="edit-desc"
                style={{ marginTop: "0.75rem" }}
              >
                <TextArea
                  id="edit-desc"
                  value={desc}
                  onChange={(_e, v) => setDesc(v)}
                />
              </FormGroup>
              <FormGroup
                label="Tags (comma-separated)"
                fieldId="edit-tags"
                style={{ marginTop: "0.75rem" }}
              >
                <TextInput
                  id="edit-tags"
                  value={tagsStr}
                  onChange={(_e, v) => setTagsStr(v)}
                />
              </FormGroup>
              <FormGroup
                label="Schema Fields (JSON)"
                fieldId="edit-schema"
                style={{ marginTop: "0.75rem" }}
              >
                <TextArea
                  id="edit-schema"
                  value={schemaStr}
                  onChange={(_e, v) => setSchemaStr(v)}
                  rows={8}
                  style={{ fontFamily: "monospace" }}
                />
              </FormGroup>
              <div style={{ marginTop: "1rem" }}>
                <Button variant="primary" onClick={handleSave}>
                  Save
                </Button>
                <Button
                  variant="link"
                  onClick={() => {
                    setEditing(false);
                    load();
                  }}
                >
                  Cancel
                </Button>
              </div>
            </>
          ) : (
            <DescriptionList isHorizontal>
              <DescriptionListGroup>
                <DescriptionListTerm>Description</DescriptionListTerm>
                <DescriptionListDescription>
                  {ds.description || "-"}
                </DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Tags</DescriptionListTerm>
                <DescriptionListDescription>
                  {ds.tags && ds.tags.length > 0
                    ? ds.tags.map((t) => (
                        <Label key={t} style={{ marginRight: 4 }}>
                          {t}
                        </Label>
                      ))
                    : "-"}
                </DescriptionListDescription>
              </DescriptionListGroup>
            </DescriptionList>
          )}
        </CardBody>
      </Card>

      {ds.schema_fields && ds.schema_fields.length > 0 && !editing && (
        <Card style={{ marginBottom: "1rem" }}>
          <CardTitle>Schema</CardTitle>
          <CardBody>
            <Table aria-label="Schema fields" variant="compact">
              <Thead>
                <Tr>
                  <Th>Column</Th>
                  <Th>Type</Th>
                </Tr>
              </Thead>
              <Tbody>
                {ds.schema_fields.map((f: SchemaField) => (
                  <Tr key={f.name}>
                    <Td dataLabel="Column">
                      <code>{f.name}</code>
                    </Td>
                    <Td dataLabel="Type">
                      <code>{f.type}</code>
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          </CardBody>
        </Card>
      )}

      <Card style={{ marginBottom: "1rem" }}>
        <CardTitle>Pipelines (from Marquez lineage)</CardTitle>
        <CardBody>
          {pipelines.length === 0 ? (
            <p>No pipelines have referenced this dataset yet.</p>
          ) : (
            <Table aria-label="Pipelines" variant="compact">
              <Thead>
                <Tr>
                  <Th>Namespace (pipeline)</Th>
                  <Th>Lineage</Th>
                </Tr>
              </Thead>
              <Tbody>
                {pipelines.map((p) => (
                  <Tr key={p.namespace}>
                    <Td dataLabel="Namespace">
                      <code>{p.namespace}</code>
                    </Td>
                    <Td dataLabel="Lineage">
                      <Button
                        variant="link"
                        isInline
                        onClick={() =>
                          navigate(
                            `/lineage?ns=${encodeURIComponent(ds.ol_namespace)}&name=${encodeURIComponent(ds.ol_name)}&pipeline=${encodeURIComponent(p.namespace)}`
                          )
                        }
                      >
                        View lineage
                      </Button>
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
        </CardBody>
      </Card>

      <Button
        variant="danger"
        onClick={handleDelete}
        style={{ marginTop: "1rem" }}
      >
        Delete Dataset
      </Button>
    </>
  );
}
