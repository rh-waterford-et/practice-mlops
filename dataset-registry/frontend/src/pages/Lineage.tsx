import React, { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  Card,
  CardBody,
  ClipboardCopy,
  FormGroup,
  FormSelect,
  FormSelectOption,
  Label,
  Title,
} from "@patternfly/react-core";
import { Dataset, listDatasets } from "../api";

declare global {
  interface Window {
    MARQUEZ_WEB_URL?: string;
  }
}

const MARQUEZ_WEB =
  window.MARQUEZ_WEB_URL ||
  "https://marquez-web-lineage.apps.rosa.catoconn-ray-et.bo0z.p3.openshiftapps.com";

export default function Lineage() {
  const [searchParams] = useSearchParams();
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [selected, setSelected] = useState("");

  const paramNs = searchParams.get("ns");
  const paramName = searchParams.get("name");
  const paramPipeline = searchParams.get("pipeline");

  useEffect(() => {
    listDatasets().then((d) => {
      setDatasets(d.datasets);
      if (paramNs && paramName) {
        const match = d.datasets.find(
          (ds) => ds.ol_namespace === paramNs && ds.ol_name === paramName
        );
        if (match) setSelected(match.id);
      }
    });
  }, [paramNs, paramName]);

  const selectedDs = datasets.find((d) => d.id === selected);

  const iframeSrc = selectedDs
    ? `${MARQUEZ_WEB}/lineage/dataset/${encodeURIComponent(selectedDs.ol_namespace)}/${encodeURIComponent(selectedDs.ol_name)}`
    : MARQUEZ_WEB;

  return (
    <>
      <Title headingLevel="h1" style={{ marginBottom: "1rem" }}>
        Lineage
      </Title>

      <FormGroup
        label="Select a dataset to view its lineage"
        fieldId="ds-select"
        style={{ maxWidth: 500, marginBottom: "1rem" }}
      >
        <FormSelect
          id="ds-select"
          value={selected}
          onChange={(_e, v) => setSelected(v)}
        >
          <FormSelectOption value="" label="-- Overview --" />
          {datasets.map((ds) => (
            <FormSelectOption
              key={ds.id}
              value={ds.id}
              label={`${ds.name} (${ds.source})`}
            />
          ))}
        </FormSelect>
      </FormGroup>

      {selectedDs && (
        <Card style={{ marginBottom: "1rem" }}>
          <CardBody>
            <p style={{ marginBottom: "0.5rem" }}>
              <strong>OL Namespace:</strong>{" "}
              <code>{selectedDs.ol_namespace}</code>
              {" | "}
              <strong>OL Name:</strong> <code>{selectedDs.ol_name}</code>
            </p>
            {paramPipeline && (
              <p style={{ marginBottom: "0.5rem" }}>
                <strong>Pipeline:</strong>{" "}
                <Label color="blue">{paramPipeline}</Label>
              </p>
            )}
            <ClipboardCopy isReadOnly>
              {`dataset:${selectedDs.ol_namespace}:${selectedDs.ol_name}`}
            </ClipboardCopy>
          </CardBody>
        </Card>
      )}

      <div
        style={{
          width: "100%",
          height: selectedDs ? "calc(100vh - 340px)" : "calc(100vh - 200px)",
          border: "1px solid var(--pf-t--global--border--color--default)",
          borderRadius: 4,
        }}
      >
        <iframe
          key={iframeSrc}
          src={iframeSrc}
          style={{ width: "100%", height: "100%", border: "none" }}
          title="Marquez Lineage"
        />
      </div>
    </>
  );
}
