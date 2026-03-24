import React, { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  Button,
  EmptyState,
  EmptyStateBody,
  Label,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  FormGroup,
  TextInput,
  TextArea,
  Title,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
  PageSection,
} from "@patternfly/react-core";
import { Table, Thead, Tr, Th, Tbody, Td } from "@patternfly/react-table";
import { PlusCircleIcon } from "@patternfly/react-icons";
import {
  Dataset,
  DatasetCreate,
  SchemaField,
  listDatasets,
  createDataset,
  deleteDataset,
} from "../api";

export default function Datasets() {
  const navigate = useNavigate();
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isCreateOpen, setCreateOpen] = useState(false);

  const [form, setForm] = useState<DatasetCreate>({
    name: "",
    source: "",
    description: "",
    tags: [],
  });
  const [tagsInput, setTagsInput] = useState("");
  const [schemaFields, setSchemaFields] = useState<SchemaField[]>([]);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listDatasets();
      setDatasets(data.datasets);
      setError(null);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const handleCreate = async () => {
    try {
      const body: DatasetCreate = {
        name: form.name,
        source: form.source,
      };
      if (form.description) body.description = form.description;
      const parsedTags = tagsInput.split(",").map((t) => t.trim()).filter(Boolean);
      if (parsedTags.length > 0) body.tags = parsedTags;
      await createDataset(body);
      setCreateOpen(false);
      setForm({ name: "", source: "", description: "", tags: [] });
      setTagsInput("");
      refresh();
    } catch (e: any) {
      setError(e.message);
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteDataset(id);
      refresh();
    } catch (e: any) {
      setError(e.message);
    }
  };

  return (
    <>
      <Title headingLevel="h1" style={{ marginBottom: "1rem" }}>
        Datasets
      </Title>

      <Toolbar>
        <ToolbarContent>
          <ToolbarItem>
            <Button
              variant="primary"
              icon={<PlusCircleIcon />}
              onClick={() => setCreateOpen(true)}
            >
              Register Dataset
            </Button>
          </ToolbarItem>
        </ToolbarContent>
      </Toolbar>

      {error && (
        <PageSection hasBodyWrapper={false}>
          <span style={{ color: "var(--pf-t--global--color--status--danger--default)" }}>
            {error}
          </span>
        </PageSection>
      )}

      {datasets.length === 0 && !loading ? (
        <EmptyState>
          <EmptyStateBody>
            No datasets registered yet. Click &quot;Register Dataset&quot; to add one.
          </EmptyStateBody>
        </EmptyState>
      ) : (
        <Table aria-label="Datasets table">
          <Thead>
            <Tr>
              <Th>Name</Th>
              <Th>Source</Th>
              <Th>Description</Th>
              <Th>Tags</Th>
              <Th>Created</Th>
              <Th>Actions</Th>
            </Tr>
          </Thead>
          <Tbody>
            {datasets.map((ds) => (
              <Tr
                key={ds.id}
                isClickable
                onRowClick={() => navigate(`/datasets/${ds.id}`)}
                style={{ cursor: "pointer" }}
              >
                <Td dataLabel="Name">{ds.name}</Td>
                <Td dataLabel="Source">
                  <code>{ds.source}</code>
                </Td>
                <Td dataLabel="Description">{ds.description || "-"}</Td>
                <Td dataLabel="Tags">
                  {ds.tags?.map((t) => (
                    <Label key={t} style={{ marginRight: 4 }}>
                      {t}
                    </Label>
                  ))}
                </Td>
                <Td dataLabel="Created">
                  {new Date(ds.created_at).toLocaleDateString()}
                </Td>
                <Td dataLabel="Actions">
                  <Button
                    variant="danger"
                    size="sm"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDelete(ds.id);
                    }}
                  >
                    Delete
                  </Button>
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      )}

      <Modal
        isOpen={isCreateOpen}
        onClose={() => setCreateOpen(false)}
        aria-label="Register Dataset"
        variant="medium"
      >
        <ModalHeader title="Register Dataset" />
        <ModalBody>
          <FormGroup label="Dataset Name" isRequired fieldId="ds-name">
            <TextInput
              id="ds-name"
              isRequired
              value={form.name}
              onChange={(_e, v) => setForm({ ...form, name: v })}
            />
          </FormGroup>
          <FormGroup
            label="Source (physical URI, e.g. postgres://host:5432/db.schema.table)"
            isRequired
            fieldId="source"
            style={{ marginTop: "0.75rem" }}
          >
            <TextInput
              id="source"
              isRequired
              value={form.source}
              onChange={(_e, v) => setForm({ ...form, source: v })}
            />
          </FormGroup>
          <FormGroup
            label="Description"
            fieldId="desc"
            style={{ marginTop: "0.75rem" }}
          >
            <TextArea
              id="desc"
              value={form.description || ""}
              onChange={(_e, v) => setForm({ ...form, description: v })}
            />
          </FormGroup>
          <FormGroup
            label="Tags (comma-separated)"
            fieldId="tags"
            style={{ marginTop: "0.75rem" }}
          >
            <TextInput
              id="tags"
              value={tagsInput}
              onChange={(_e, v) => setTagsInput(v)}
            />
          </FormGroup>
        </ModalBody>
        <ModalFooter>
          <Button
            variant="primary"
            onClick={handleCreate}
            isDisabled={!form.name || !form.source}
          >
            Register
          </Button>
          <Button variant="link" onClick={() => setCreateOpen(false)}>
            Cancel
          </Button>
        </ModalFooter>
      </Modal>
    </>
  );
}
