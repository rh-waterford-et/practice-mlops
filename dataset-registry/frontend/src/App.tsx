import React from "react";
import { BrowserRouter, Routes, Route, useNavigate, useLocation } from "react-router-dom";
import {
  Brand,
  Masthead,
  MastheadMain,
  MastheadBrand,
  Nav,
  NavItem,
  NavList,
  Page,
  PageSidebar,
  PageSidebarBody,
  PageSection,
} from "@patternfly/react-core";

import Datasets from "./pages/Datasets";
import DatasetDetail from "./pages/DatasetDetail";
import Lineage from "./pages/Lineage";

const ROUTES = [
  { path: "/datasets", label: "Datasets" },
  { path: "/lineage", label: "Lineage" },
];

function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();

  const header = (
    <Masthead>
      <MastheadMain>
        <MastheadBrand>
          <span
            style={{ color: "#EE0000", fontWeight: 700, fontSize: "1.1rem", cursor: "pointer" }}
            onClick={() => navigate("/datasets")}
          >
            Dataset Registry
          </span>
        </MastheadBrand>
      </MastheadMain>
    </Masthead>
  );

  const nav = (
    <Nav>
      <NavList>
        {ROUTES.map((r) => (
          <NavItem
            key={r.path}
            isActive={location.pathname.startsWith(r.path)}
            onClick={() => navigate(r.path)}
          >
            {r.label}
          </NavItem>
        ))}
      </NavList>
    </Nav>
  );

  const sidebar = (
    <PageSidebar>
      <PageSidebarBody>{nav}</PageSidebarBody>
    </PageSidebar>
  );

  return (
    <Page masthead={header} sidebar={sidebar}>
      <PageSection hasBodyWrapper={false}>
        <Routes>
          <Route path="/datasets" element={<Datasets />} />
          <Route path="/datasets/:id" element={<DatasetDetail />} />
          <Route path="/lineage" element={<Lineage />} />
          <Route path="*" element={<Datasets />} />
        </Routes>
      </PageSection>
    </Page>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AppLayout />
    </BrowserRouter>
  );
}
