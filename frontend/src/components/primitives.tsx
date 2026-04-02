import clsx from "clsx";
import type { PropsWithChildren, ReactNode } from "react";

export function Button({
  children,
  className,
  variant = "secondary",
  ...props
}: PropsWithChildren<
  React.ButtonHTMLAttributes<HTMLButtonElement> & {
    variant?: "primary" | "secondary" | "ghost" | "danger";
  }
>) {
  return (
    <button
      {...props}
      className={clsx(
        "inline-flex items-center justify-center rounded-md px-3 py-2 text-body-md font-semibold transition disabled:cursor-not-allowed disabled:opacity-50",
        variant === "primary" && "bg-primary-gradient text-surface hover:brightness-110",
        variant === "secondary" && "bg-surface-variant text-on-surface hover:bg-surface-container-highest",
        variant === "ghost" && "text-primary hover:underline",
        variant === "danger" && "text-error hover:underline",
        className,
      )}
    >
      {children}
    </button>
  );
}

export function SurfaceCard({
  children,
  className,
}: PropsWithChildren<{ className?: string }>) {
  return (
    <section className={clsx("rounded-lg bg-surface-container p-4 ghost-border", className)}>
      {children}
    </section>
  );
}

export function MetricCard({
  label,
  value,
  detail,
  accent = "primary",
}: {
  label: string;
  value: ReactNode;
  detail: string;
  accent?: "primary" | "tertiary" | "warning";
}) {
  const accentClass =
    accent === "tertiary"
      ? "border-l-tertiary"
      : accent === "warning"
        ? "border-l-warning"
        : "border-l-primary";
  return (
    <SurfaceCard className={clsx("border-l-2", accentClass)}>
      <div className="text-label-sm uppercase tracking-[0.1em] text-on-surface-variant">{label}</div>
      <div className="mt-1 font-mono text-3xl font-bold text-on-surface">{value}</div>
      <div className="mt-1 text-label-sm text-on-surface-variant">{detail}</div>
    </SurfaceCard>
  );
}

export function SectionHeader({
  title,
  description,
  right,
}: {
  title: string;
  description: string;
  right?: ReactNode;
}) {
  return (
    <div className="mb-5 flex flex-wrap items-end justify-between gap-4">
      <div>
        <h1 className="text-display-sm font-extrabold">{title}</h1>
        <p className="mt-1 text-body-md text-on-surface-variant">{description}</p>
      </div>
      {right}
    </div>
  );
}

export function EmptyState({
  title,
  detail,
}: {
  title: string;
  detail: string;
}) {
  return (
    <div className="rounded-lg bg-surface-container-low p-6 text-center ghost-border">
      <div className="text-title-sm font-semibold">{title}</div>
      <div className="mt-2 text-body-md text-on-surface-variant">{detail}</div>
    </div>
  );
}

export function InputField(
  props: React.InputHTMLAttributes<HTMLInputElement> & { label: string; className?: string },
) {
  const { label, className, id, ...rest } = props;
  return (
    <label className={clsx("grid gap-1 text-body-md", className)} htmlFor={id}>
      <span className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">{label}</span>
      <input
        id={id}
        className="rounded-md border border-outline-variant bg-surface-container-lowest px-3 py-2 text-body-md text-on-surface focus:border-primary focus:outline-none"
        {...rest}
      />
    </label>
  );
}

export function SelectField(
  props: React.SelectHTMLAttributes<HTMLSelectElement> & {
    label: string;
    className?: string;
    children: ReactNode;
  },
) {
  const { label, className, id, children, ...rest } = props;
  return (
    <label className={clsx("grid gap-1 text-body-md", className)} htmlFor={id}>
      <span className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">{label}</span>
      <select
        id={id}
        className="rounded-md border border-outline-variant bg-surface-container-lowest px-3 py-2 text-body-md text-on-surface focus:border-primary focus:outline-none"
        {...rest}
      >
        {children}
      </select>
    </label>
  );
}

export function TextAreaField(
  props: React.TextareaHTMLAttributes<HTMLTextAreaElement> & { label: string; className?: string },
) {
  const { label, className, id, ...rest } = props;
  return (
    <label className={clsx("grid gap-1 text-body-md", className)} htmlFor={id}>
      <span className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">{label}</span>
      <textarea
        id={id}
        className="rounded-md border border-outline-variant bg-surface-container-lowest px-3 py-2 text-body-md text-on-surface focus:border-primary focus:outline-none"
        {...rest}
      />
    </label>
  );
}

export function StatusBadge({
  text,
  tone,
}: {
  text: string;
  tone: "neutral" | "success" | "warning" | "error" | "active";
}) {
  const classes =
    tone === "success"
      ? "bg-success/10 text-success"
      : tone === "warning"
        ? "bg-warning/10 text-warning"
        : tone === "error"
          ? "bg-error/10 text-error"
          : tone === "active"
            ? "bg-tertiary/10 text-tertiary"
            : "bg-surface-variant text-on-surface-variant";
  return <span className={clsx("status-pill", classes)}>{text}</span>;
}
